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
use std::collections::{HashMap, HashSet, VecDeque};
use std::error::Error;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

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

fn parse_feed_info(
    path: &Path,
) -> Result<Option<gtfs_structures::FeedInfo>, Box<dyn Error + Send + Sync>> {
    let feed_info_path = path.join("feed_info.txt");
    if !feed_info_path.exists() {
        return Ok(None);
    }
    let file = File::open(feed_info_path)?;
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(BufReader::new(file));

    let headers = rdr.headers()?.clone();
    let get_field = |row: &csv::StringRecord, field_name: &str| -> Option<String> {
        headers
            .iter()
            .position(|h| h == field_name)
            .and_then(|idx| row.get(idx))
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    };

    let mut records = rdr.into_records();
    if let Some(record_result) = records.next() {
        let record = record_result?;
        let name = get_field(&record, "feed_publisher_name").unwrap_or_default();
        let url = get_field(&record, "feed_publisher_url").unwrap_or_default();
        let lang = get_field(&record, "feed_lang").unwrap_or_default();

        let parse_date = |s: Option<String>| -> Option<NaiveDate> {
            s.and_then(|d| NaiveDate::parse_from_str(&d, "%Y%m%d").ok())
        };

        let start_date = parse_date(get_field(&record, "feed_start_date"));
        let end_date = parse_date(get_field(&record, "feed_end_date"));
        let version = get_field(&record, "feed_version");
        let contact_email = get_field(&record, "feed_contact_email");
        let contact_url = get_field(&record, "feed_contact_url");
        let default_lang = get_field(&record, "default_lang");

        Ok(Some(gtfs_structures::FeedInfo {
            name,
            url,
            lang,
            start_date,
            end_date,
            version,
            contact_email,
            contact_url,
            default_lang,
        }))
    } else {
        Ok(None)
    }
}

fn parse_agencies(
    path: &Path,
) -> Result<Vec<gtfs_structures::Agency>, Box<dyn Error + Send + Sync>> {
    let agency_path = path.join("agency.txt");
    let file = File::open(agency_path)?;
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(BufReader::new(file));

    let headers = rdr.headers()?.clone();
    let get_field = |row: &csv::StringRecord, field_name: &str| -> Option<String> {
        headers
            .iter()
            .position(|h| h == field_name)
            .and_then(|idx| row.get(idx))
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    };

    let mut agencies = Vec::new();
    for record_result in rdr.records() {
        let record = record_result?;
        let id = get_field(&record, "agency_id");
        let name = get_field(&record, "agency_name").unwrap_or_default();
        let url = get_field(&record, "agency_url").unwrap_or_default();
        let timezone = get_field(&record, "agency_timezone").unwrap_or_default();
        let lang = get_field(&record, "agency_lang");
        let phone = get_field(&record, "agency_phone");
        let fare_url = get_field(&record, "agency_fare_url");
        let email = get_field(&record, "agency_email");

        agencies.push(gtfs_structures::Agency {
            id,
            name,
            url,
            timezone,
            lang,
            phone,
            fare_url,
            email,
        });
    }
    Ok(agencies)
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

struct AgencyPartition {
    agency_idx: usize,
    raw_agency_id: String,
    sanitized_folder_name: String,
    path: PathBuf,
    target_route_ids: HashSet<String>,
}

struct PartitionWriterPool {
    file_name: String,
    headers: csv::StringRecord,
    partitions: Vec<PathBuf>,
    open_writers: HashMap<usize, csv::Writer<BufWriter<File>>>,
    access_order: VecDeque<usize>,
    max_open: usize,
}

impl PartitionWriterPool {
    fn new(
        file_name: &str,
        headers: csv::StringRecord,
        partitions: Vec<PathBuf>,
        max_open: usize,
    ) -> Self {
        Self {
            file_name: file_name.to_string(),
            headers,
            partitions,
            open_writers: HashMap::new(),
            access_order: VecDeque::new(),
            max_open,
        }
    }

    fn write_record(&mut self, partition_idx: usize, record: &csv::StringRecord) -> Result<()> {
        if !self.open_writers.contains_key(&partition_idx) {
            if self.open_writers.len() >= self.max_open {
                if let Some(oldest_idx) = self.access_order.pop_front() {
                    if let Some(mut wtr) = self.open_writers.remove(&oldest_idx) {
                        wtr.flush()?;
                    }
                }
            }

            let path = self.partitions[partition_idx].join(&self.file_name);
            let file_exists = path.exists();
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(&path)?;
            let mut wtr = csv::WriterBuilder::new()
                .has_headers(!file_exists)
                .from_writer(BufWriter::new(file));
            if !file_exists {
                wtr.write_record(&self.headers)?;
            }
            self.open_writers.insert(partition_idx, wtr);
        } else {
            if let Some(pos) = self
                .access_order
                .iter()
                .position(|&idx| idx == partition_idx)
            {
                self.access_order.remove(pos);
            }
        }

        self.access_order.push_back(partition_idx);
        let wtr = self.open_writers.get_mut(&partition_idx).unwrap();
        wtr.write_record(record)?;
        Ok(())
    }

    fn flush_all(&mut self) -> Result<()> {
        for wtr in self.open_writers.values_mut() {
            wtr.flush()?;
        }
        Ok(())
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

    println!("First read feed_info and also agency id by parsing the raw csv data.");
    let feed_info_opt = parse_feed_info(Path::new(&path))?;
    let agencies = parse_agencies(Path::new(&path))?;

    let default_lang = feed_info_opt
        .as_ref()
        .and_then(|fi| fi.default_lang.clone())
        .or_else(|| Some("de".to_string()));

    let conn_pool = arc_conn_pool.as_ref();

    println!("Inserting agencies globally for {}", feed_id);
    {
        let mut agency_id_already_done: HashSet<Option<String>> = HashSet::new();
        let mut conn = conn_pool.get().await?;

        for agency in &agencies {
            use catenary::schema::gtfs::agencies::dsl::agencies;

            if !agency_id_already_done.contains(&agency.id) {
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

                agency_id_already_done.insert(agency.id.clone());
            }
        }
    }

    println!("Reading global GTFS (lightweight) for service optimization...");
    let mut global_gtfs = gtfs_structures::GtfsReader::default()
        .read_shapes(false)
        .read_stop_times(false)
        .read(&path)
        .context("Failed to read global GTFS via gtfs_structures")?;

    println!("Running global service optimization...");
    maple_syrup::service_optimisation::optimise_services(&mut global_gtfs);

    println!("Inserting optimized calendar files into postgres...");
    calendar_into_postgres(
        &global_gtfs,
        feed_id,
        Arc::clone(&arc_conn_pool),
        chateau_id,
        attempt_id,
    )
    .await?;

    // OPTIMIZATION 1: Extract ONLY the needed service IDs to release massive GTFS collections early.
    let mut trip_to_service_id: HashMap<CompactString, CompactString> = HashMap::with_capacity(global_gtfs.trips.len());
    for (trip_id, trip) in &global_gtfs.trips {
        trip_to_service_id.insert(
            CompactString::from(trip_id.as_str()),
            CompactString::from(trip.service_id.as_str()),
        );
    }

    // Free the global feed data structures BEFORE streaming, retaining only calendar/calendar_dates.
    global_gtfs.trips.clear();
    global_gtfs.trips.shrink_to_fit();
    global_gtfs.stops.clear();
    global_gtfs.stops.shrink_to_fit();
    global_gtfs.routes.clear();
    global_gtfs.routes.shrink_to_fit();

    // Force Jemalloc to purge thread caches to reduce fragmentation.
    #[cfg(not(target_env = "msvc"))]
    {
        let _ = tikv_jemalloc_ctl::epoch::advance();
    }

    let routes_path = Path::new(&path).join("routes.txt");
    let routes_file = File::open(&routes_path)?;
    let mut routes_rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(BufReader::new(routes_file));
    let routes_headers = routes_rdr.headers()?.clone();
    let r_route_id_idx = routes_headers
        .iter()
        .position(|h| h == "route_id")
        .context("routes.txt missing route_id")?;
    let r_agency_id_idx = routes_headers
        .iter()
        .position(|h| h == "agency_id")
        .context("routes.txt missing agency_id")?;

    let mut all_routes_records: Vec<(String, String, csv::StringRecord)> = Vec::new();
    for rec_res in routes_rdr.records() {
        let rec = rec_res?;
        let route_id = rec
            .get(r_route_id_idx)
            .unwrap_or_default()
            .trim()
            .to_string();
        let agency_id = rec
            .get(r_agency_id_idx)
            .unwrap_or_default()
            .trim()
            .to_string();
        all_routes_records.push((route_id, agency_id, rec));
    }

    let split_dir = Path::new(&path).join("split_agencies");
    if split_dir.exists() {
        fs::remove_dir_all(&split_dir)
            .with_context(|| format!("Failed to clear old split dir {}", split_dir.display()))?;
    }
    fs::create_dir_all(&split_dir)?;

    let mut global_stop_ids_to_route_types: HashMap<String, Vec<i16>> = HashMap::new();
    let mut global_stop_ids_to_route_ids: HashMap<String, Vec<String>> = HashMap::new();

    let num_agencies = agencies.len();
    let mut agency_partitions = Vec::new();
    for (agency_idx, agency) in agencies.iter().enumerate() {
        let raw_agency_id = agency.id.as_deref().unwrap_or("");
        let sanitized_agency = format!("{}_{}", agency_idx, sanitize_agency_id(raw_agency_id));
        let agency_path = split_dir.join(&sanitized_agency);

        let mut target_route_ids = HashSet::new();
        let mut agency_routes = Vec::new();

        for (route_id, route_agency_id, record) in &all_routes_records {
            let is_match = if num_agencies == 1 {
                true
            } else {
                route_agency_id == raw_agency_id
                    || (route_agency_id.is_empty() && raw_agency_id.is_empty())
            };

            if is_match {
                // Keep target_route_ids as String here for agency_partitions storage
                target_route_ids.insert(route_id.clone());
                agency_routes.push(record.clone());
            }
        }

        if target_route_ids.is_empty() {
            println!("Skipping agency index {}, no active routes", agency_idx);
            continue;
        }

        fs::create_dir_all(&agency_path)?;

        {
            let mut routes_file = File::create(agency_path.join("routes.txt"))?;
            let mut wtr = csv::WriterBuilder::new()
                .has_headers(true)
                .from_writer(routes_file);
            wtr.write_record(&routes_headers)?;
            for route_rec in &agency_routes {
                wtr.write_record(route_rec)?;
            }
            wtr.flush()?;
        }

        agency_partitions.push(AgencyPartition {
            agency_idx,
            raw_agency_id: raw_agency_id.to_string(),
            sanitized_folder_name: sanitized_agency,
            path: agency_path,
            target_route_ids,
        });
    }

    let num_partitions = agency_partitions.len();
    let partition_paths: Vec<PathBuf> = agency_partitions.iter().map(|p| p.path.clone()).collect();

    // OPTIMIZATION 2: Switch mapping structs to CompactString mapping structs
    let mut route_id_to_partition_indices: HashMap<CompactString, Vec<usize>> = HashMap::new();
    for (part_idx, partition) in agency_partitions.iter().enumerate() {
        for route_id in &partition.target_route_ids {
            route_id_to_partition_indices
                .entry(CompactString::from(route_id.as_str()))
                .or_default()
                .push(part_idx);
        }
    }

    let mut target_shape_ids_by_partition: Vec<HashSet<CompactString>> = vec![HashSet::new(); num_partitions];
    let mut target_service_ids_by_partition: Vec<HashSet<CompactString>> = vec![HashSet::new(); num_partitions];

    let mut trip_id_to_partition_indices: HashMap<CompactString, Vec<usize>> = HashMap::new();

    {
        let trips_path = Path::new(&path).join("trips.txt");
        let trips_file = File::open(&trips_path)?;
        let mut trips_rdr = csv::ReaderBuilder::new()
            .has_headers(true)
            .flexible(true)
            .from_reader(BufReader::new(trips_file));

        let trips_headers = trips_rdr.headers()?.clone();
        let t_trip_id_idx = trips_headers
            .iter()
            .position(|h| h == "trip_id")
            .context("trips.txt missing trip_id")?;
        let t_route_id_idx = trips_headers
            .iter()
            .position(|h| h == "route_id")
            .context("trips.txt missing route_id")?;
        let t_shape_id_idx = trips_headers.iter().position(|h| h == "shape_id");
        let t_service_id_idx = trips_headers
            .iter()
            .position(|h| h == "service_id")
            .context("trips.txt missing service_id")?;

        let mut trips_pool =
            PartitionWriterPool::new("trips.txt", trips_headers, partition_paths.clone(), 32);

        let mut record = csv::StringRecord::new();
        while trips_rdr.read_record(&mut record)? {
            let route_id = record.get(t_route_id_idx).unwrap_or_default().trim();
            if let Some(part_indices) = route_id_to_partition_indices.get(route_id) {
                let trip_id = record.get(t_trip_id_idx).unwrap_or_default().trim();
                
                // Map the service using our extracted CompactString cache
                let service_id = trip_to_service_id
                    .get(trip_id)
                    .map(|s| s.as_str())
                    .unwrap_or_else(|| record.get(t_service_id_idx).unwrap_or_default().trim());

                let mut new_record = csv::StringRecord::new();
                for (idx, field) in record.iter().enumerate() {
                    if idx == t_service_id_idx {
                        new_record.push_field(service_id);
                    } else {
                        new_record.push_field(field);
                    }
                }

                for &part_idx in part_indices {
                    if !service_id.is_empty() {
                        // OPTIMIZATION 3: Check existence before allocation
                        if !target_service_ids_by_partition[part_idx].contains(service_id) {
                            target_service_ids_by_partition[part_idx].insert(CompactString::from(service_id));
                        }
                    }

                    if let Some(shape_idx) = t_shape_id_idx {
                        let shape_id = record.get(shape_idx).unwrap_or_default().trim();
                        if !shape_id.is_empty() {
                            if !target_shape_ids_by_partition[part_idx].contains(shape_id) {
                                target_shape_ids_by_partition[part_idx].insert(CompactString::from(shape_id));
                            }
                        }
                    }

                    trip_id_to_partition_indices
                        .entry(CompactString::from(trip_id))
                        .or_default()
                        .push(part_idx);

                    trips_pool.write_record(part_idx, &new_record)?;
                }
            }
        }
        trips_pool.flush_all()?;
    }

    let mut target_stop_ids_by_partition: Vec<HashSet<CompactString>> = vec![HashSet::new(); num_partitions];

    {
        let stop_times_path = Path::new(&path).join("stop_times.txt");
        let stop_times_file = File::open(&stop_times_path)?;
        let mut stop_times_rdr = csv::ReaderBuilder::new()
            .has_headers(true)
            .flexible(true)
            .from_reader(BufReader::new(stop_times_file));

        let stop_times_headers = stop_times_rdr.headers()?.clone();
        let st_trip_id_idx = stop_times_headers
            .iter()
            .position(|h| h == "trip_id")
            .context("stop_times.txt missing trip_id")?;
        let st_stop_id_idx = stop_times_headers
            .iter()
            .position(|h| h == "stop_id")
            .context("stop_times.txt missing stop_id")?;

        let mut stop_times_pool = PartitionWriterPool::new(
            "stop_times.txt",
            stop_times_headers,
            partition_paths.clone(),
            32,
        );

        let mut record = csv::StringRecord::new();
        while stop_times_rdr.read_record(&mut record)? {
            let trip_id = record.get(st_trip_id_idx).unwrap_or_default().trim();
            if let Some(part_indices) = trip_id_to_partition_indices.get(trip_id) {
                let stop_id = record.get(st_stop_id_idx).unwrap_or_default().trim();
                for &part_idx in part_indices {
                    if !target_stop_ids_by_partition[part_idx].contains(stop_id) {
                        target_stop_ids_by_partition[part_idx].insert(CompactString::from(stop_id));
                    }
                    stop_times_pool.write_record(part_idx, &record)?;
                }
            }
        }
        stop_times_pool.flush_all()?;
    }

    {
        let stops_path = Path::new(&path).join("stops.txt");
        let stops_file = File::open(&stops_path)?;
        let mut stops_rdr = csv::ReaderBuilder::new()
            .has_headers(true)
            .flexible(true)
            .from_reader(BufReader::new(stops_file));

        let stops_headers = stops_rdr.headers()?.clone();
        let s_stop_id_idx = stops_headers
            .iter()
            .position(|h| h == "stop_id")
            .context("stops.txt missing stop_id")?;
        let s_parent_station_idx = stops_headers.iter().position(|h| h == "parent_station");

        let mut stop_id_to_parent: HashMap<CompactString, CompactString> = HashMap::new();
        let mut record = csv::StringRecord::new();
        while stops_rdr.read_record(&mut record)? {
            let stop_id = record.get(s_stop_id_idx).unwrap_or_default().trim();
            if let Some(parent_idx) = s_parent_station_idx {
                let parent = record.get(parent_idx).unwrap_or_default().trim();
                if !parent.is_empty() {
                    stop_id_to_parent.insert(CompactString::from(stop_id), CompactString::from(parent));
                }
            }
        }

        let mut parent_ids_to_fetch_by_partition: Vec<HashSet<CompactString>> = vec![HashSet::new(); num_partitions];
        for part_idx in 0..num_partitions {
            for stop_id in &target_stop_ids_by_partition[part_idx] {
                if let Some(parent) = stop_id_to_parent.get(stop_id.as_str()) {
                    if !parent_ids_to_fetch_by_partition[part_idx].contains(parent) {
                        parent_ids_to_fetch_by_partition[part_idx].insert(parent.clone());
                    }
                }
            }
        }

        let stops_file2 = File::open(&stops_path)?;
        let mut stops_rdr2 = csv::ReaderBuilder::new()
            .has_headers(true)
            .flexible(true)
            .from_reader(BufReader::new(stops_file2));

        let mut stops_pool =
            PartitionWriterPool::new("stops.txt", stops_headers, partition_paths.clone(), 32);

        let mut record = csv::StringRecord::new();
        while stops_rdr2.read_record(&mut record)? {
            let stop_id = record.get(s_stop_id_idx).unwrap_or_default().trim();
            for part_idx in 0..num_partitions {
                if target_stop_ids_by_partition[part_idx].contains(stop_id)
                    || parent_ids_to_fetch_by_partition[part_idx].contains(stop_id)
                {
                    stops_pool.write_record(part_idx, &record)?;
                }
            }
        }
        stops_pool.flush_all()?;
    }

    {
        let shapes_path = Path::new(&path).join("shapes.txt");
        if shapes_path.exists() {
            let has_shapes = target_shape_ids_by_partition.iter().any(|s| !s.is_empty());
            if has_shapes {
                let shapes_file = File::open(&shapes_path)?;
                let mut shapes_rdr = csv::ReaderBuilder::new()
                    .has_headers(true)
                    .flexible(true)
                    .from_reader(BufReader::new(shapes_file));

                let shapes_headers = shapes_rdr.headers()?.clone();
                let sh_shape_id_idx = shapes_headers
                    .iter()
                    .position(|h| h == "shape_id")
                    .context("shapes.txt missing shape_id")?;

                let mut shapes_pool = PartitionWriterPool::new(
                    "shapes.txt",
                    shapes_headers,
                    partition_paths.clone(),
                    32,
                );

                let mut record = csv::StringRecord::new();
                while shapes_rdr.read_record(&mut record)? {
                    let shape_id = record.get(sh_shape_id_idx).unwrap_or_default().trim();
                    for part_idx in 0..num_partitions {
                        if target_shape_ids_by_partition[part_idx].contains(shape_id) {
                            shapes_pool.write_record(part_idx, &record)?;
                        }
                    }
                }
                shapes_pool.flush_all()?;
            }
        }
    }

    {
        let frequencies_path = Path::new(&path).join("frequencies.txt");
        if frequencies_path.exists() {
            let has_frequencies = !trip_id_to_partition_indices.is_empty();
            if has_frequencies {
                let frequencies_file = File::open(&frequencies_path)?;
                let mut frequencies_rdr = csv::ReaderBuilder::new()
                    .has_headers(true)
                    .flexible(true)
                    .from_reader(BufReader::new(frequencies_file));

                let frequencies_headers = frequencies_rdr.headers()?.clone();
                let f_trip_id_idx = frequencies_headers
                    .iter()
                    .position(|h| h == "trip_id")
                    .context("frequencies.txt missing trip_id")?;

                let mut frequencies_pool = PartitionWriterPool::new(
                    "frequencies.txt",
                    frequencies_headers,
                    partition_paths.clone(),
                    32,
                );

                let mut record = csv::StringRecord::new();
                while frequencies_rdr.read_record(&mut record)? {
                    let trip_id = record.get(f_trip_id_idx).unwrap_or_default().trim();
                    if let Some(part_indices) = trip_id_to_partition_indices.get(trip_id) {
                        for &part_idx in part_indices {
                            frequencies_pool.write_record(part_idx, &record)?;
                        }
                    }
                }
                frequencies_pool.flush_all()?;
            }
        }
    }

    // Force Jemalloc to purge thread caches to reduce fragmentation.
    #[cfg(not(target_env = "msvc"))]
    {
        let _ = tikv_jemalloc_ctl::epoch::advance();
    }

    for (part_idx, partition) in agency_partitions.iter().enumerate() {
        let agency_path = &partition.path;
        let sanitized_agency = &partition.sanitized_folder_name;
        let agency_id = sanitized_agency.clone();
        let agency = &agencies[partition.agency_idx];

        let target_service_ids = &target_service_ids_by_partition[part_idx];

        let calendar_path = agency_path.join("calendar.txt");
        if !global_gtfs.calendar.is_empty() && !target_service_ids.is_empty() {
            let mut calendar_wtr = csv::WriterBuilder::new()
                .has_headers(true)
                .from_writer(File::create(&calendar_path)?);
            calendar_wtr.write_record(&[
                "service_id",
                "monday",
                "tuesday",
                "wednesday",
                "thursday",
                "friday",
                "saturday",
                "sunday",
                "start_date",
                "end_date",
            ])?;
            for (service_id, cal) in &global_gtfs.calendar {
                if target_service_ids.contains(service_id) {
                    calendar_wtr.write_record(&[
                        service_id.as_str(),
                        if cal.monday { "1" } else { "0" },
                        if cal.tuesday { "1" } else { "0" },
                        if cal.wednesday { "1" } else { "0" },
                        if cal.thursday { "1" } else { "0" },
                        if cal.friday { "1" } else { "0" },
                        if cal.saturday { "1" } else { "0" },
                        if cal.sunday { "1" } else { "0" },
                        &cal.start_date.format("%Y%m%d").to_string(),
                        &cal.end_date.format("%Y%m%d").to_string(),
                    ])?;
                }
            }
            calendar_wtr.flush()?;
        }

        let calendar_dates_path = agency_path.join("calendar_dates.txt");
        if !global_gtfs.calendar_dates.is_empty() && !target_service_ids.is_empty() {
            let mut calendar_dates_wtr = csv::WriterBuilder::new()
                .has_headers(true)
                .from_writer(File::create(&calendar_dates_path)?);
            calendar_dates_wtr.write_record(&["service_id", "date", "exception_type"])?;
            for (service_id, dates) in &global_gtfs.calendar_dates {
                if target_service_ids.contains(service_id) {
                    for date in dates {
                        let exc_type = match date.exception_type {
                            gtfs_structures::Exception::Added => "1",
                            gtfs_structures::Exception::Deleted => "2",
                        };
                        calendar_dates_wtr.write_record(&[
                            service_id.as_str(),
                            &date.date.format("%Y%m%d").to_string(),
                            exc_type,
                        ])?;
                    }
                }
            }
            calendar_dates_wtr.flush()?;
        }

        {
            let agency_headers = vec![
                "agency_id",
                "agency_name",
                "agency_url",
                "agency_timezone",
                "agency_lang",
                "agency_phone",
                "agency_fare_url",
            ];
            let mut agency_wtr = csv::WriterBuilder::new()
                .has_headers(true)
                .from_writer(File::create(agency_path.join("agency.txt"))?);
            agency_wtr.write_record(&agency_headers)?;
            agency_wtr.write_record(&[
                agency.id.as_deref().unwrap_or(""),
                &agency.name,
                &agency.url,
                &agency.timezone,
                agency.lang.as_deref().unwrap_or(""),
                agency.phone.as_deref().unwrap_or(""),
                agency.fare_url.as_deref().unwrap_or(""),
            ])?;
            agency_wtr.flush()?;
        }

        let feed_info_path = Path::new(&path).join("feed_info.txt");
        if feed_info_path.exists() {
            let _ = fs::copy(&feed_info_path, agency_path.join("feed_info.txt"));
        }

        println!(
            "Processing agency partition {} for {}",
            sanitized_agency, feed_id
        );

        let mut agency_gtfs = gtfs_structures::GtfsReader::default()
            .read_shapes(false)
            .read_stop_times(false)
            .read(agency_path.to_str().unwrap())?;

        agency_gtfs =
            faster_stop_time_reader_injection(agency_gtfs, &agency_path.join("stop_times.txt"))?;

        let mut agency_gtfs = crate::gtfs_handlers::gtfs_de_cleanup::gtfs_de_cleanup(agency_gtfs);

        let today = chrono::Utc::now().naive_utc().date();
        let mut agency_gtfs = minimum_day_filter(agency_gtfs, today - chrono::Duration::days(7));

        if agency_gtfs.trips.is_empty() {
            println!("Skipping agency split {}, no active trips", agency_id);
            continue;
        }

        let reduction = maple_syrup::reduce(&agency_gtfs);

        // immediately clear trip.stop_times after reduction
        for trip in agency_gtfs.trips.values_mut() {
            trip.stop_times.clear();
            trip.stop_times.shrink_to_fit();
        }

        let mut route_to_direction_patterns: HashMap<String, Vec<&maple_syrup::DirectionPattern>> =
            HashMap::new();
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

        // then clear trips entirely
        agency_gtfs.trips.clear();
        agency_gtfs.trips.shrink_to_fit();

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
                        interpolated_time_since_start: stop_sequence.interpolated_time_since_start,
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
                        route_id: route_id_transform(feed_id, compressed_trip_raw.route_id.clone()),
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
                    continuous_pickup: continuous_pickup_drop_off_to_i16(&route.continuous_pickup),
                };
                route_pg
            })
            .collect();

        for route_chunk in &agency_gtfs.routes.iter().chunks(256) {
            let mut insertable_elastic: Vec<elasticsearch::http::request::JsonBody<_>> = Vec::new();
            for (route_id, route) in route_chunk {
                let envelope = agency_shapes_minimised.as_ref().and_then(|reader| {
                    route_ids_to_shape_ids
                        .get(route_id)
                        .and_then(|shape_ids| compute_shape_envelope(reader, shape_ids.iter()))
                });

                let mut short_name_translations_shortened_locales_elastic: HashMap<String, String> =
                    HashMap::new();
                let mut long_name_translations_shortened_locales_elastic: HashMap<String, String> =
                    HashMap::new();
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
                            if let (Some(lat), Some(lon)) = (end_stop.latitude, end_stop.longitude)
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

            if !insertable_elastic.is_empty() {
                if let Some(client) = elasticclient {
                    let response = client
                        .bulk(elasticsearch::BulkParts::Index("routes"))
                        .body(insertable_elastic)
                        .send()
                        .await;

                    if let Ok(res) = response {
                        if let Ok(response_body) = res.json::<serde_json::Value>().await {
                            if response_body.get("errors").map(|x| x.as_bool()).flatten()
                                == Some(true)
                            {
                                eprintln!(
                                    "ElasticSearch bulk insert for routes had errors: {:?}",
                                    response_body
                                );
                            }
                        }
                    } else if let Err(e) = response {
                        eprintln!(
                            "Failed to send routes bulk insert to ElasticSearch: {:?}",
                            e
                        );
                    }
                }
            }
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

        let _ = fs::remove_dir_all(&agency_path);

        // Drop large structures to release memory as early as possible.
        drop(agency_gtfs);
        drop(route_to_direction_patterns);
        drop(reduction);

        // Force Jemalloc to flush thread-local caches back to the OS.
        #[cfg(not(target_env = "msvc"))]
        {
            let _ = tikv_jemalloc_ctl::epoch::advance();
        }
    }

    println!("Loading global GTFS metadata (lightweight) for stops/hull/feed_info...");
    let temp_global_dir = Path::new(&path).join("temp_global_gtfs");
    fs::create_dir_all(&temp_global_dir)?;

    let link_or_copy = |src: &Path, dst: &Path| -> Result<(), Box<dyn Error + Send + Sync>> {
        if src.exists() {
            #[cfg(unix)]
            {
                let _ = fs::remove_file(dst);
                std::os::unix::fs::symlink(src, dst)?;
            }
            #[cfg(not(unix))]
            {
                fs::copy(src, dst)?;
            }
        }
        Ok(())
    };

    link_or_copy(
        &Path::new(&path).join("agency.txt"),
        &temp_global_dir.join("agency.txt"),
    )?;
    link_or_copy(
        &Path::new(&path).join("stops.txt"),
        &temp_global_dir.join("stops.txt"),
    )?;
    link_or_copy(
        &Path::new(&path).join("routes.txt"),
        &temp_global_dir.join("routes.txt"),
    )?;
    if Path::new(&path).join("feed_info.txt").exists() {
        link_or_copy(
            &Path::new(&path).join("feed_info.txt"),
            &temp_global_dir.join("feed_info.txt"),
        )?;
    }

    let mut dummy_trips = File::create(temp_global_dir.join("trips.txt"))?;
    writeln!(dummy_trips, "route_id,service_id,trip_id")?;
    drop(dummy_trips);

    let global_gtfs = gtfs_structures::GtfsReader::default()
        .read_shapes(false)
        .read_stop_times(false)
        .read(temp_global_dir.to_str().unwrap())
        .context("Failed to read global GTFS via gtfs_structures")?;

    let _ = fs::remove_dir_all(&temp_global_dir);

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
        &global_stop_ids_to_route_types,
        &global_stop_ids_to_route_ids,
        &stop_id_to_children_ids,
        None,
        &default_lang,
        elasticclient,
    )
    .await?;

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
