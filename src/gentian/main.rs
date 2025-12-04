use ahash::AHashMap as HashMap;
use ahash::AHashSet as HashSet;
use anyhow::{Context, Result};
use catenary::models::{
    Agency, Calendar, CompressedTrip as DbCompressedTrip, ItineraryPatternRow, Route, Stop,
};
use catenary::postgres_tools::{CatenaryPostgresPool, make_async_pool};

use catenary::routing_common::transit_graph::BoundaryPoint;
use catenary::routing_common::transit_graph::{
    CompressedTrip, DagEdge, DirectionPattern, EdgeEntry, EdgeType, ExternalTransfer,
    GlobalPatternIndex, Manifest, OsmLink, PartitionBoundary, PartitionTimetableData,
    StaticTransfer, TimeDeltaSequence, TimetableData, TransferChunk, TransitEdge, TransitPartition,
    TransitStop, TripPattern, load_bincode, save_bincode,
};
use clap::Parser;
use diesel::prelude::*;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use geo::prelude::*;

use geo::{ConcaveHull, MultiPoint, Point};
use rand::prelude::*;
use rayon::prelude::*;

use std::collections::BinaryHeap;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::time::Instant;

use std::sync::Arc;

use rstar::RTree;
use rstar::primitives::GeomWithData;

pub mod clustering;
pub mod connectivity;
pub mod osm;
pub mod reduce_borders;
pub mod repro_hub;
#[cfg(test)]
pub mod repro_large_file;
#[cfg(test)]
pub mod test_border_patterns;
#[cfg(test)]
pub mod test_global_pruning;
pub mod test_hub;
#[cfg(test)]
pub mod test_local_patterns;
#[cfg(test)]
pub mod test_reduce_borders;

pub mod cluster_global;
pub mod extract;
pub mod station_matching;
#[cfg(test)]
pub mod test_stitching;
pub mod test_trip_based;
pub mod tp;
pub mod trip_based;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Chateau ID to process (comma-separated for multiple)
    #[arg(short, long)]
    chateau: Option<String>,

    /// Directory containing OSM chunks
    #[arg(long, default_value = "osm_chunks")]
    osm_chunks: PathBuf,

    /// Output directory for generated GTFS chunks
    #[arg(short, long)]
    output: PathBuf,

    /// Target cluster size (number of stops)
    #[arg(long, default_value = "1500")]
    cluster_size: usize,

    /// Run in stitch mode (rebuild global graph from chunks)
    #[arg(long)]
    stitch: bool,

    /// Subcommand to run specific phases
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(clap::Subcommand, Debug)]
enum Commands {
    /// Extract data from DB and update shards
    Extract {
        /// Chateau IDs to process (comma-separated)
        #[arg(short, long)]
        chateau: String,
    },
    /// Run global clustering on shards
    Cluster,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    // Connect to DB
    let pool = make_async_pool()
        .await
        .map_err(|e| anyhow::anyhow!(e))
        .context("Failed to create DB pool")?;
    let pool = Arc::new(pool);

    // Create output directory
    tokio::fs::create_dir_all(&args.output)
        .await
        .context("Failed to create output dir")?;

    if let Some(cmd) = &args.command {
        match cmd {
            Commands::Extract { chateau } => {
                let chateaux_list: Vec<String> =
                    chateau.split(',').map(|s| s.trim().to_string()).collect();
                extract::run_extraction(pool, chateaux_list, &args.output).await?;
            }
            Commands::Cluster => {
                cluster_global::run_global_clustering(&args.output)?;
            }
        }
    } else {
        // Legacy/Default behavior
        if let Some(chateau_str) = &args.chateau {
            println!("Starting Gentian for Chateaux: {}", chateau_str);
            // Run generation or stitch
            if args.stitch {
                stitch_graph(&args).await?;
            } else {
                generate_chunks(&args, pool).await?;
                // Always stitch after generation to ensure global connectivity is up to date
                stitch_graph(&args).await?;
            }
        } else {
            println!("Please provide --chateau or a subcommand.");
        }
    }

    Ok(())
}

async fn generate_chunks(args: &Args, pool: Arc<CatenaryPostgresPool>) -> Result<()> {
    let mut conn = pool.get().await.context("Failed to get DB connection")?;

    let chateaux_list: Vec<String> = args
        .chateau
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();
    println!(
        "Processing {} chateaux: {:?}",
        chateaux_list.len(),
        chateaux_list
    );

    // Load existing manifest if it exists
    let manifest_path = args.output.join("manifest.json");
    let mut existing_manifest = if manifest_path.exists() {
        let file = File::open(&manifest_path).context("Failed to open manifest.json")?;
        let reader = BufReader::new(file);
        serde_json::from_reader(reader).unwrap_or_else(|_| Manifest {
            chateau_to_partitions: std::collections::HashMap::new(),
            partition_to_chateaux: std::collections::HashMap::new(),
            partition_boundaries: std::collections::HashMap::new(),
        })
    } else {
        Manifest {
            chateau_to_partitions: std::collections::HashMap::new(),
            partition_to_chateaux: std::collections::HashMap::new(),
            partition_boundaries: std::collections::HashMap::new(),
        }
    };

    // Validate existing manifest: remove partitions whose files are missing
    let mut missing_partitions = HashSet::new();
    for p_list in existing_manifest.chateau_to_partitions.values() {
        for &pid in p_list {
            let path = args.output.join(format!("transit_chunk_{}.pbf", pid));
            if !path.exists() {
                missing_partitions.insert(pid);
            }
        }
    }

    if !missing_partitions.is_empty() {
        println!(
            "Warning: Found {} partitions in manifest with missing files. Removing them from manifest.",
            missing_partitions.len()
        );

        // Remove from chateau_to_partitions
        for p_list in existing_manifest.chateau_to_partitions.values_mut() {
            p_list.retain(|p| !missing_partitions.contains(p));
        }
        // Remove empty entries
        existing_manifest
            .chateau_to_partitions
            .retain(|_, v| !v.is_empty());

        // Remove from partition_to_chateaux
        existing_manifest
            .partition_to_chateaux
            .retain(|k, _| !missing_partitions.contains(k));

        // Remove from partition_boundaries
        existing_manifest
            .partition_boundaries
            .retain(|k, _| !missing_partitions.contains(k));
    }

    let mut used_ids: HashSet<u32> = existing_manifest
        .partition_boundaries
        .keys()
        .cloned()
        .collect();

    // 2. Identify Overlapping Partitions & Expand Chateaux List
    println!("Identifying overlapping partitions...");

    let mut chateaux_to_process: HashSet<String> = chateaux_list.into_iter().collect();
    let mut processed_chateaux: HashSet<String> = HashSet::new();
    let mut partitions_to_remove: HashSet<u32> = HashSet::new();

    // Initial fetch of stops for requested chateaux to determine BBox
    let mut initial_stops: Vec<Stop> = Vec::new();
    for chateau_id in &chateaux_to_process {
        use catenary::schema::gtfs::stops::dsl::{chateau as stop_chateau, stops};
        let stops_chunk: Vec<Stop> = stops
            .filter(stop_chateau.eq(chateau_id))
            .select(Stop::as_select())
            .load(&mut conn)
            .await?;
        initial_stops.extend(stops_chunk);
    }

    // Overlapping partition check removed. Partitions are allowed to overlap.
    /*
    if !initial_stops.is_empty() {
        // ... (removed logic)
    }
    */

    // Also add partitions that contain any of the requested chateaux (to ensure complete replacement)
    for chateau_id in &chateaux_to_process {
        if let Some(pids) = existing_manifest.chateau_to_partitions.get(chateau_id) {
            for pid in pids {
                partitions_to_remove.insert(*pid);
            }
        }
    }

    // Expand chateaux list from the identified partitions
    for pid in &partitions_to_remove {
        if let Some(chateaux) = existing_manifest.partition_to_chateaux.get(pid) {
            for c in chateaux {
                chateaux_to_process.insert(c.clone());
            }
        }
    }

    println!("Expanded Chateaux List: {:?}", chateaux_to_process);
    println!("Partitions to Replace: {:?}", partitions_to_remove);

    // 3. Fetch All Data for ALL Chateaux (Expanded List)
    println!(
        "Fetching data for {} chateaux...",
        chateaux_to_process.len()
    );

    let mut db_stops: Vec<Stop> = Vec::new();
    let mut db_trips: Vec<DbCompressedTrip> = Vec::new();
    let mut db_patterns: Vec<ItineraryPatternRow> = Vec::new();
    let mut db_calendar: Vec<Calendar> = Vec::new();
    let mut db_routes: Vec<Route> = Vec::new();
    let mut db_agencies: Vec<Agency> = Vec::new();

    // Optimization: We already fetched stops for the initial list. Reuse them?
    // Yes, but we need to be careful about duplicates if we re-fetch.
    // Simplest approach: Re-fetch everything for the final list, or filter.
    // Let's just use the `processed_chateaux` set to track what we fetched.

    // Add initial stops to db_stops
    db_stops.extend(initial_stops);
    // Mark initial chateaux as processed (for stops only? No, we need trips too).
    // Wait, we ONLY fetched stops for initial list. We still need trips/patterns for them.
    // So we should iterate `chateaux_to_process`. If it was in initial list, we skip fetching stops (already have them), but fetch others.

    // Actually, `initial_stops` contains stops for `chateaux_to_process` (initial set).
    // We need to fetch stops for the *added* chateaux.
    // And trips/etc for *all* chateaux.

    // Let's rebuild `db_stops` cleanly.
    // Map: Chateau -> Vec<Stop>
    let mut stops_by_chateau: HashMap<String, Vec<Stop>> = HashMap::new();
    for s in db_stops.drain(..) {
        stops_by_chateau
            .entry(s.chateau.clone())
            .or_default()
            .push(s);
    }

    for chateau_id in &chateaux_to_process {
        println!("  - Fetching data for {}", chateau_id);

        // Stops (if not already fetched)
        if !stops_by_chateau.contains_key(chateau_id) {
            use catenary::schema::gtfs::stops::dsl::{chateau as stop_chateau, stops};
            let stops_chunk: Vec<Stop> = stops
                .filter(stop_chateau.eq(chateau_id))
                .select(Stop::as_select())
                .load(&mut conn)
                .await?;
            stops_by_chateau.insert(chateau_id.clone(), stops_chunk);
        }

        // Flatten stops back to db_stops later

        // Trips
        use catenary::schema::gtfs::trips_compressed::dsl::{
            chateau as trip_chateau, trips_compressed,
        };
        let trips_chunk: Vec<DbCompressedTrip> = trips_compressed
            .filter(trip_chateau.eq(chateau_id))
            .select(DbCompressedTrip::as_select())
            .load(&mut conn)
            .await?;
        db_trips.extend(trips_chunk);

        // Itinerary Patterns
        use catenary::schema::gtfs::itinerary_pattern::dsl::{
            chateau as pattern_chateau, itinerary_pattern,
        };
        let patterns_chunk: Vec<ItineraryPatternRow> = itinerary_pattern
            .filter(pattern_chateau.eq(chateau_id))
            .select(ItineraryPatternRow::as_select())
            .load(&mut conn)
            .await?;
        db_patterns.extend(patterns_chunk);

        // Calendar
        use catenary::schema::gtfs::calendar::dsl::{calendar, chateau as cal_chateau};
        let calendar_chunk: Vec<Calendar> = calendar
            .filter(cal_chateau.eq(chateau_id))
            .select(Calendar::as_select())
            .load(&mut conn)
            .await?;
        db_calendar.extend(calendar_chunk);

        // Routes
        use catenary::schema::gtfs::routes::dsl::{chateau as route_chateau, routes};
        let routes_chunk: Vec<Route> = routes
            .filter(route_chateau.eq(chateau_id))
            .select(Route::as_select())
            .load(&mut conn)
            .await?;
        db_routes.extend(routes_chunk);

        // Agencies
        use catenary::schema::gtfs::agencies::dsl::{agencies, chateau as agency_chateau};
        let agencies_chunk: Vec<Agency> = agencies
            .filter(agency_chateau.eq(chateau_id))
            .select(Agency::as_select())
            .load(&mut conn)
            .await?;
        db_agencies.extend(agencies_chunk);
    }

    // Flatten stops map back to db_stops
    for (_, stops) in stops_by_chateau {
        db_stops.extend(stops);
    }

    println!("Total Fetched (Primary):");
    println!("  - Stops: {}", db_stops.len());
    println!("  - Trips: {}", db_trips.len());
    println!("  - Patterns: {}", db_patterns.len());
    println!("  - Patterns: {}", db_patterns.len());
    println!("  - Routes: {}", db_routes.len());
    println!("  - Agencies: {}", db_agencies.len());

    // 2b. Fetch Referenced External Stops
    let mut loaded_stops: HashSet<(String, String)> = db_stops
        .iter()
        .map(|s| (s.chateau.clone(), s.gtfs_id.clone()))
        .collect();

    let mut missing_stops: HashSet<(String, String)> = HashSet::new();
    for row in &db_patterns {
        let key = (row.chateau.clone(), row.stop_id.to_string());
        if !loaded_stops.contains(&key) {
            missing_stops.insert(key);
        }
    }

    if !missing_stops.is_empty() {
        println!(
            "Fetching {} referenced external stops...",
            missing_stops.len()
        );
        let mut by_chateau: HashMap<String, Vec<String>> = HashMap::new();
        for (c, s) in missing_stops {
            by_chateau.entry(c).or_default().push(s);
        }

        for (chateau_id, stop_ids) in by_chateau {
            use catenary::schema::gtfs::stops::dsl::{chateau, gtfs_id, stops};
            for chunk in stop_ids.chunks(1000) {
                let extra_stops: Vec<Stop> = stops
                    .filter(chateau.eq(&chateau_id))
                    .filter(gtfs_id.eq_any(chunk))
                    .select(Stop::as_select())
                    .load(&mut conn)
                    .await?;
                db_stops.extend(extra_stops);
            }
        }
        println!(
            "  - Total Stops after fetching external: {}",
            db_stops.len()
        );
    }

    // Run Station Matching
    println!("Running Station Matching...");
    for chateau_id in &chateaux_to_process {
        let stops_for_chateau: Vec<Stop> = db_stops
            .iter()
            .filter(|s| s.chateau == *chateau_id)
            .cloned()
            .collect();

        if !stops_for_chateau.is_empty() {
            station_matching::match_stops_to_stations(&mut conn, chateau_id, &stops_for_chateau)
                .await?;
        }
    }
    println!("Station Matching Complete.");

    // Load Mappings
    use catenary::models::{Station, StopMapping};
    use catenary::schema::gtfs::stations::dsl::{station_id as station_table_id, stations};
    use catenary::schema::gtfs::stop_mappings::dsl::{feed_id as mapping_feed_id, stop_mappings};

    let all_mappings: Vec<StopMapping> = stop_mappings
        .filter(mapping_feed_id.eq_any(&chateaux_to_process))
        .load(&mut conn)
        .await?;

    let mut stop_to_station_map: HashMap<(String, String), String> = HashMap::new();
    for m in all_mappings {
        stop_to_station_map.insert((m.feed_id, m.stop_id), m.station_id);
    }

    // Load Stations (Only those referenced by mappings)
    let referenced_station_ids: Vec<String> = stop_to_station_map.values().cloned().collect();
    // Dedup
    let referenced_station_ids: HashSet<String> = referenced_station_ids.into_iter().collect();
    let referenced_station_ids: Vec<String> = referenced_station_ids.into_iter().collect();

    let all_stations: Vec<Station> = stations
        .filter(station_table_id.eq_any(&referenced_station_ids))
        .load(&mut conn)
        .await?;

    let station_map: HashMap<String, Station> = all_stations
        .into_iter()
        .map(|s| (s.station_id.clone(), s))
        .collect();

    // Create db_stations vector and mappings
    let mut db_stations: Vec<Station> = station_map.values().cloned().collect();
    // Sort for determinism
    db_stations.sort_by(|a, b| a.station_id.cmp(&b.station_id));

    let station_id_to_idx: HashMap<String, usize> = db_stations
        .iter()
        .enumerate()
        .map(|(i, s)| (s.station_id.clone(), i))
        .collect();

    // Map Stop Index -> Station Index
    let mut stop_idx_to_station_idx: Vec<usize> = Vec::with_capacity(db_stops.len());
    let mut station_idx_to_stop_indices: HashMap<usize, Vec<usize>> = HashMap::new();

    for (i, stop) in db_stops.iter().enumerate() {
        let station_id = stop_to_station_map.get(&(stop.chateau.clone(), stop.gtfs_id.clone()));
        let s_idx = if let Some(sid) = station_id {
            *station_id_to_idx.get(sid).unwrap_or(&0)
        } else {
            0
        };
        stop_idx_to_station_idx.push(s_idx);
        station_idx_to_stop_indices
            .entry(s_idx)
            .or_default()
            .push(i);
    }

    let route_map: HashMap<(String, String), Route> = db_routes
        .into_iter()
        .map(|r| ((r.chateau.clone(), r.route_id.clone()), r))
        .collect();

    // Map Route ID -> Timezone
    // 1. Map Agency ID -> Timezone
    let mut agency_timezone_map: HashMap<(String, String), String> = HashMap::new(); // (Chateau, AgencyID) -> Timezone
    for agency in &db_agencies {
        agency_timezone_map.insert(
            (agency.chateau.clone(), agency.agency_id.clone()),
            agency.agency_timezone.clone(),
        );
    }

    // 2. Map Route ID -> Timezone
    let mut route_timezone_map: HashMap<(String, String), String> = HashMap::new();
    for route in route_map.values() {
        let tz = if let Some(agency_id) = &route.agency_id {
            agency_timezone_map
                .get(&(route.chateau.clone(), agency_id.clone()))
                .cloned()
        } else {
            // Fallback: Try to find ANY agency for this chateau?
            // Or just use the first one found for the chateau.
            db_agencies
                .iter()
                .find(|a| a.chateau == route.chateau)
                .map(|a| a.agency_timezone.clone())
        };

        if let Some(t) = tz {
            route_timezone_map.insert((route.chateau.clone(), route.route_id.clone()), t);
        }
    }

    // 3. Process Data
    // Map (Chateau, GTFS_ID) -> Global Index
    let stop_id_map: HashMap<(String, String), usize> = db_stops
        .iter()
        .enumerate()
        .map(|(i, s)| ((s.chateau.clone(), s.gtfs_id.clone()), i))
        .collect();

    let mut pattern_rows_map: HashMap<(String, String), Vec<&ItineraryPatternRow>> = HashMap::new();
    for row in &db_patterns {
        pattern_rows_map
            .entry((row.chateau.clone(), row.itinerary_pattern_id.clone()))
            .or_default()
            .push(row);
    }
    for rows in pattern_rows_map.values_mut() {
        rows.sort_by_key(|r| r.stop_sequence);
    }

    let mut global_time_deltas: Vec<TimeDeltaSequence> = Vec::new();
    let mut time_deltas_map: HashMap<Vec<u32>, u32> = HashMap::new();

    let mut processed_patterns: Vec<ProcessedPattern> = Vec::new();

    let mut trips_by_pattern: HashMap<(String, String), Vec<&DbCompressedTrip>> = HashMap::new();
    for trip in &db_trips {
        trips_by_pattern
            .entry((trip.chateau.clone(), trip.itinerary_pattern_id.clone()))
            .or_default()
            .push(trip);
    }

    let mut service_ids: Vec<String> = Vec::new();
    let mut service_id_map: HashMap<String, u32> = HashMap::new();

    let mut global_timezones: Vec<String> = Vec::new();
    let mut timezone_map: HashMap<String, u32> = HashMap::new();

    // Build Adjacency Graph for Clustering
    // Edge weight = Number of trips passing between two stops
    let mut adjacency: HashMap<(usize, usize), u32> = HashMap::new();

    let mut merged_patterns: HashMap<((String, String), Vec<u32>), ProcessedPattern> =
        HashMap::new();

    for ((chateau_id, pattern_id), trips) in trips_by_pattern {
        if let Some(rows) = pattern_rows_map.get(&(chateau_id.clone(), pattern_id)) {
            let stop_indices: Vec<u32> = rows
                .iter()
                .filter_map(|r| {
                    stop_id_map
                        .get(&(r.chateau.clone(), r.stop_id.to_string()))
                        .map(|&i| i as u32)
                })
                .collect();

            let station_indices: Vec<u32> = stop_indices
                .iter()
                .map(|&idx| stop_idx_to_station_idx[idx as usize] as u32)
                .collect();

            if stop_indices.len() != rows.len() {
                continue;
            }

            // Filter out long-distance or high-speed trips for clustering
            let mut total_distance = 0.0;
            for i in 0..stop_indices.len() - 1 {
                let u = stop_indices[i] as usize;
                let v = stop_indices[i + 1] as usize;
                if let (Some(p1), Some(p2)) = (&db_stops[u].point, &db_stops[v].point) {
                    total_distance += utils::haversine_distance(p1.y, p1.x, p2.y, p2.x);
                    // let _: () = p1;
                }
            }

            let start_time = rows
                .get(0)
                .and_then(|r| r.departure_time_since_start)
                .unwrap_or(0);
            let end_time = rows
                .last()
                .and_then(|r| r.arrival_time_since_start)
                .unwrap_or(0);
            let duration = if end_time > start_time {
                end_time - start_time
            } else {
                0
            };

            let avg_speed_kmh = if duration > 0 {
                (total_distance / duration as f64) * 3.6
            } else {
                0.0
            };

            let is_long_distance = total_distance > 200_000.0; // 200km
            let is_high_speed = avg_speed_kmh > 50.0 && total_distance > 100_000.0; // 50km/h & 100km

            if !is_long_distance && !is_high_speed {
                // Add to Adjacency Graph
                let trip_count = trips.len() as u32;
                for i in 0..stop_indices.len() - 1 {
                    let u_stop = stop_indices[i] as usize;
                    let v_stop = stop_indices[i + 1] as usize;

                    let u_station = stop_idx_to_station_idx[u_stop];
                    let v_station = stop_idx_to_station_idx[v_stop];

                    if u_station != v_station {
                        let (min, max) = if u_station < v_station {
                            (u_station, v_station)
                        } else {
                            (v_station, u_station)
                        };
                        *adjacency.entry((min, max)).or_default() += trip_count;
                    }
                }
            }

            let mut deltas: Vec<u32> = Vec::new();
            // Stop 0: Travel=0, Dwell = Dep - Arr
            let s0_arr = rows[0].arrival_time_since_start.unwrap_or(0);
            let s0_dep = rows[0].departure_time_since_start.unwrap_or(s0_arr);
            deltas.push(0); // Travel to 0
            deltas.push((s0_dep - s0_arr) as u32); // Dwell at 0

            for i in 1..rows.len() {
                let s_prev_dep = rows[i - 1].departure_time_since_start.unwrap_or(0);
                let s_curr_arr = rows[i].arrival_time_since_start.unwrap_or(0);
                let s_curr_dep = rows[i].departure_time_since_start.unwrap_or(s_curr_arr);

                let travel = if s_curr_arr > s_prev_dep {
                    s_curr_arr - s_prev_dep
                } else {
                    0
                };
                let dwell = if s_curr_dep > s_curr_arr {
                    s_curr_dep - s_curr_arr
                } else {
                    0
                };
                deltas.push(travel as u32);
                deltas.push(dwell as u32);
            }

            // Timezone
            let route_id = trips[0].route_id.clone();
            let tz_str = route_timezone_map
                .get(&(chateau_id.clone(), route_id.clone()))
                .cloned()
                .unwrap_or_else(|| "UTC".to_string());

            let tz_idx = if let Some(&idx) = timezone_map.get(&tz_str) {
                idx
            } else {
                let idx = global_timezones.len() as u32;
                global_timezones.push(tz_str.clone());
                timezone_map.insert(tz_str, idx);
                idx
            };

            let delta_ptr = if let Some(&ptr) = time_deltas_map.get(&deltas) {
                ptr
            } else {
                let ptr = global_time_deltas.len() as u32;
                time_deltas_map.insert(deltas.clone(), ptr);
                global_time_deltas.push(TimeDeltaSequence { deltas });
                ptr
            };

            let mut p_trips = Vec::new();
            for trip in &trips {
                let s_idx = if let Some(&idx) = service_id_map.get(trip.service_id.as_str()) {
                    idx
                } else {
                    let idx = service_ids.len() as u32;
                    service_ids.push(trip.service_id.to_string());
                    service_id_map.insert(trip.service_id.to_string(), idx);
                    idx
                };

                let service_mask = calculate_service_mask(trip.service_id.as_str(), &db_calendar);

                p_trips.push(CompressedTrip {
                    gtfs_trip_id: trip.trip_id.clone(),
                    service_mask,
                    start_time: trip.start_time,
                    time_delta_idx: delta_ptr,
                    service_idx: s_idx,
                    bikes_allowed: trip.bikes_allowed as u32,
                    wheelchair_accessible: trip.wheelchair_accessible as u32,
                });
            }

            // Map Timezone
            let route_id = trips[0].route_id.clone();
            let tz_str = route_timezone_map
                .get(&(chateau_id.clone(), route_id.clone()))
                .cloned()
                .unwrap_or_else(|| "UTC".to_string());

            let tz_idx = if let Some(&idx) = timezone_map.get(&tz_str) {
                idx
            } else {
                let idx = global_timezones.len() as u32;
                global_timezones.push(tz_str.clone());
                timezone_map.insert(tz_str, idx);
                idx
            };

            // Merge into ProcessedPattern
            let key = (
                (chateau_id.clone(), route_id.clone()),
                station_indices.clone(),
            );

            let entry = merged_patterns
                .entry(key)
                .or_insert_with(|| ProcessedPattern {
                    chateau: trips[0].chateau.clone(),
                    route_id,
                    stop_indices: station_indices,
                    trips: Vec::new(),
                    timezone_idx: tz_idx,
                });
            entry.trips.extend(p_trips);
        }
    }

    for pat in merged_patterns.values_mut() {
        pat.trips.sort_by_key(|t| t.start_time);
    }

    let processed_patterns: Vec<ProcessedPattern> = merged_patterns.into_values().collect();

    println!("Processed {} patterns", processed_patterns.len());

    // 4. Merge-Based Clustering (Moved Before Hub Identification)
    println!("Clustering stops (Merge-Based)...");
    let clusters = merge_based_clustering(db_stations.len(), &adjacency, args.cluster_size);

    // A stop is "used" if it appears in any pattern (which means it is in adjacency? No, adjacency is edges)
    // Actually, we should check if it's referenced by any trip.
    // For now, assume all stations are relevant or will be filtered out if empty.

    println!("Initial Clusters: {}", clusters.len());

    // Filter out clusters that contain only unused stops
    // A stop is "used" if it appears in any pattern (which means it is in adjacency? No, adjacency is edges)
    // Better: Check if stop is in any processed_pattern.
    let mut active_stops = HashSet::new();
    for pat in &processed_patterns {
        for &idx in &pat.stop_indices {
            active_stops.insert(idx as usize);
        }
    }

    let clusters: Vec<Vec<usize>> = clusters
        .into_iter()
        .filter(|c| c.iter().any(|s| active_stops.contains(s)))
        .collect();

    println!(
        "Retained {} clusters after filtering unused stops",
        clusters.len()
    );

    // 4b. Reduce Borders by Merging (Post-Processing)
    // Target max size for merged clusters: 1500
    let clusters = reduce_borders_by_merging(clusters, &adjacency, 1500, db_stops.len());
    println!(
        "Reduced to {} clusters after border reduction",
        clusters.len()
    );

    // --- PASS 1: Regional Hubs (Super-Clusters) ---
    // Removed super-clustering to enforce partition size cap.
    // We treat each cluster as its own region for hub identification.
    let final_super_clusters: Vec<Vec<usize>> = (0..clusters.len()).map(|i| vec![i]).collect();
    println!("  > Skipped Super-Clustering to enforce partition size cap.");

    // Generate unique partition IDs for each cluster
    let mut cluster_id_to_partition_id: Vec<u32> = Vec::with_capacity(clusters.len());
    let mut rng = rand::rng();

    // Initialize used_ids with existing partitions from manifest
    let mut used_ids: HashSet<u32> = HashSet::new();
    for p_list in existing_manifest.chateau_to_partitions.values() {
        for &pid in p_list {
            if !partitions_to_remove.contains(&pid) {
                used_ids.insert(pid);
            }
        }
    }

    for _ in 0..clusters.len() {
        let mut pid = rng.random::<u32>();
        while used_ids.contains(&pid) {
            pid = rng.random::<u32>();
        }
        used_ids.insert(pid);
        cluster_id_to_partition_id.push(pid);
    }

    // Analyze cluster sizes
    let mut sizes: Vec<usize> = clusters.iter().map(|c| c.len()).collect();
    sizes.sort_unstable();
    if !sizes.is_empty() {
        println!(
            "Cluster sizes: Min={}, Max={}, Median={}",
            sizes[0],
            sizes[sizes.len() - 1],
            sizes[sizes.len() / 2]
        );
        println!(
            "Top 10 largest clusters: {:?}",
            sizes.iter().rev().take(10).collect::<Vec<_>>()
        );
    }

    // 5. Generate Chunks per Cluster
    // Identify Intra-Cluster Border Nodes
    let mut stop_to_cluster: Vec<usize> = vec![0; db_stops.len()];
    for (c_idx, c_stops) in clusters.iter().enumerate() {
        for &s_idx in c_stops {
            stop_to_cluster[s_idx] = c_idx;
        }
    }

    // Build Global Adjacency Signatures (Directed)
    let mut global_adjacency_signatures: HashMap<(usize, usize), (String, String)> = HashMap::new();
    for pat in &processed_patterns {
        for i in 0..pat.stop_indices.len() - 1 {
            let u = pat.stop_indices[i] as usize;
            let v = pat.stop_indices[i + 1] as usize;
            // Keep the first route ID found for simplicity
            global_adjacency_signatures
                .entry((u, v))
                .or_insert_with(|| (pat.chateau.clone(), pat.route_id.clone()));
        }
    }

    let mut border_stops: HashSet<usize> = HashSet::new();
    let mut cross_partition_signatures: Vec<((usize, usize), (String, String))> = Vec::new();

    for ((u, v), _) in &adjacency {
        if stop_to_cluster[*u] != stop_to_cluster[*v] {
            border_stops.insert(*u);
            border_stops.insert(*v);

            // Check for directed edges in both directions
            if let Some(sig) = global_adjacency_signatures.get(&(*u, *v)) {
                cross_partition_signatures.push(((*u, *v), sig.clone()));
            }
            if let Some(sig) = global_adjacency_signatures.get(&(*v, *u)) {
                cross_partition_signatures.push(((*v, *u), sig.clone()));
            }
        }
    }
    println!(
        "Identified {} intra-cluster border nodes",
        border_stops.len()
    );

    // Identify Hubs (Time-Dependent Centrality)
    println!("Identifying Hubs (Hierarchical Approach)...");
    let mut hubs: HashSet<usize> = HashSet::new();

    // Helper to run identification on a subset
    let run_hub_identification = |subset_name: &str,
                                  subset_stop_indices: &[usize],
                                  subset_patterns: &[&ProcessedPattern],
                                  sample_size_override: Option<usize>|
     -> Result<HashSet<usize>> {
        if subset_stop_indices.is_empty() || subset_patterns.is_empty() {
            println!("  - Skipping {} (No data)", subset_name);
            return Ok(HashSet::new());
        }

        // 1. Map Global -> Local
        let mut global_to_local: HashMap<usize, u32> = HashMap::new();
        let mut local_stops: Vec<Stop> = Vec::with_capacity(subset_stop_indices.len());

        for (local_idx, &global_idx) in subset_stop_indices.iter().enumerate() {
            global_to_local.insert(global_idx, local_idx as u32);
            local_stops.push(db_stops[global_idx].clone());
        }

        // 2. Re-index Patterns
        let mut local_patterns: Vec<ProcessedPattern> = Vec::with_capacity(subset_patterns.len());
        for pat in subset_patterns {
            let mut new_indices = Vec::with_capacity(pat.stop_indices.len());
            let mut valid = true;
            for &global_idx in &pat.stop_indices {
                if let Some(&local_idx) = global_to_local.get(&(global_idx as usize)) {
                    new_indices.push(local_idx);
                } else {
                    // This pattern uses a stop not in the subset?
                    // This shouldn't happen if we filtered correctly, but if it does, we should probably skip or truncate?
                    // For Regional pass, all stops should be in chateau.
                    // For Global pass, we include all stops used by patterns.
                    valid = false;
                    break;
                }
            }
            if valid {
                let mut new_pat = ProcessedPattern {
                    chateau: pat.chateau.clone(),
                    route_id: pat.route_id.clone(),
                    stop_indices: new_indices,
                    trips: pat.trips.clone(),
                    timezone_idx: pat.timezone_idx,
                };
                local_patterns.push(new_pat);
            }
        }

        // 3. Determine Parameters
        let num_subset_stops = local_stops.len();
        let dynamic_top_k = (num_subset_stops / 100).min(1).max(100);

        let sample_size = sample_size_override.unwrap_or_else(|| 100 + (num_subset_stops / 10));

        println!(
            "  - Pass: {} | Stops: {} | Patterns: {} | Samples: {} | Top-K: {}",
            subset_name,
            num_subset_stops,
            local_patterns.len(),
            sample_size,
            dynamic_top_k
        );

        // 4. Run Identification
        let local_hubs = identify_hubs_time_independent(
            &local_stops,
            &local_patterns,
            &global_time_deltas,
            &db_calendar,
            sample_size,
            dynamic_top_k,
        );

        // 5. Map Local -> Global
        let mut global_hubs = HashSet::new();
        for local_idx in local_hubs {
            if local_idx < subset_stop_indices.len() {
                global_hubs.insert(subset_stop_indices[local_idx]);
            }
        }

        Ok(global_hubs)
    };

    println!("  > Starting Regional Pass (Super-Clusters)...");
    for (i, sc) in final_super_clusters.iter().enumerate() {
        // Collect all stops in this super cluster
        let mut region_stop_indices = Vec::new();
        for &c_idx in sc {
            region_stop_indices.extend(&clusters[c_idx]);
        }

        // Filter Patterns
        // We need patterns that touch ANY stop in this region.
        // Optimization: Precompute pattern -> stop set? Or just iterate.
        // Iterating processed_patterns (which can be large) for every super cluster might be slow if many SCs.
        // But with 50k stops per SC, we have few SCs.

        let region_stop_set: HashSet<u32> =
            region_stop_indices.iter().map(|&idx| idx as u32).collect();

        let region_patterns: Vec<&ProcessedPattern> = processed_patterns
            .iter()
            .filter(|p| p.stop_indices.iter().any(|s| region_stop_set.contains(s)))
            .collect();

        let region_hubs = run_hub_identification(
            &format!("Region {} ({} clusters)", i, sc.len()),
            &region_stop_indices,
            &region_patterns,
            None,
        )?;

        hubs.extend(region_hubs);
    }

    // --- PASS 2: Global Hubs (Long Distance / Rail) ---
    println!("  > Starting Global Pass...");

    // Filter Patterns: Route Type 2 (Rail) or maybe others?
    // Let's include Route Type 2 (Rail), 1 (Subway) if it crosses regions?
    // Route types: 2 (Rail), 3 (Bus), 1 (Subway/Metro).
    // Long distance bus is hard to distinguish from local bus by route_type alone (both 3).
    // But we can check if a pattern spans multiple chateaux?
    // Or just rely on Rail (2) for now as a proxy for "Backbone".
    // Also include patterns that cross chateau boundaries?
    // Let's stick to Route Type 2 (Rail) as the primary "Global" layer for now,
    // plus maybe patterns that have long average stop distances?
    // For simplicity: Route Type 2.

    let global_patterns: Vec<&ProcessedPattern> = processed_patterns
        .iter()
        .filter(|p| {
            if let Some(route) = route_map.get(&(p.chateau.clone(), p.route_id.clone())) {
                // Type 2 = Rail, Type 101-108 = various rails.
                // GTFS route types: 2 is Rail.
                route.route_type == 2
            } else {
                false
            }
        })
        .collect();

    if !global_patterns.is_empty() {
        // Collect all stops used by these patterns
        let mut global_stop_indices_set = HashSet::new();
        for p in &global_patterns {
            for &idx in &p.stop_indices {
                global_stop_indices_set.insert(idx as usize);
            }
        }
        let mut global_stop_indices: Vec<usize> = global_stop_indices_set.into_iter().collect();
        global_stop_indices.sort(); // Deterministic order

        let global_layer_hubs = run_hub_identification(
            "Global Layer (Rail)",
            &global_stop_indices,
            &global_patterns,
            None, // Use default scaling
        )?;

        println!("    -> Identified {} global hubs", global_layer_hubs.len());
        hubs.extend(global_layer_hubs);
    } else {
        println!("    -> No global patterns found (Route Type 2)");
    }

    println!("Total Unique Hubs Identified: {}", hubs.len());

    let mut cross_partition_edges: Vec<((usize, usize), DagEdge)> = Vec::new();
    let mut partitions: HashMap<u32, TransitPartition> = HashMap::new();
    let mut global_to_partition_map: HashMap<usize, (u32, u32)> = HashMap::new();

    let mut all_global_nodes: HashMap<u32, Vec<TransitStop>> = HashMap::new();
    let mut partition_boundaries: HashMap<u32, PartitionBoundary> = HashMap::new();

    let mut chunk_cache = ChunkCache::new(50);

    for (cluster_id, cluster_stop_indices) in clusters.iter().enumerate() {
        let mut relevant_patterns = Vec::new();
        let mut relevant_stop_indices: HashSet<u32> = HashSet::new();

        for &idx in cluster_stop_indices {
            relevant_stop_indices.insert(idx as u32);
        }

        for pat in &processed_patterns {
            let touches_cluster = pat
                .stop_indices
                .iter()
                .any(|idx| relevant_stop_indices.contains(idx));
            if touches_cluster {
                relevant_patterns.push(pat);
                for &idx in &pat.stop_indices {
                    relevant_stop_indices.insert(idx);
                }
            }
        }

        let mut local_stop_map: HashMap<u32, u64> = HashMap::new();
        let mut chunk_stops: Vec<TransitStop> = Vec::new();
        let mut osm_links: Vec<OsmLink> = Vec::new();

        let mut chateau_ids: Vec<String> = Vec::new();
        let mut chateau_map: HashMap<String, u32> = HashMap::new();

        let mut get_chateau_idx = |chateau: &str| -> u32 {
            if let Some(&idx) = chateau_map.get(chateau) {
                idx
            } else {
                let idx = chateau_ids.len() as u32;
                chateau_ids.push(chateau.to_string());
                chateau_map.insert(chateau.to_string(), idx);
                idx
            }
        };

        let mut sorted_relevant_stations: Vec<u32> = relevant_stop_indices.into_iter().collect();
        sorted_relevant_stations.sort();

        for (local_idx, &global_idx) in sorted_relevant_stations.iter().enumerate() {
            local_stop_map.insert(global_idx, local_idx as u64);

            // If this partition owns the station, record it in the map
            if stop_to_cluster[global_idx as usize] == cluster_id {
                global_to_partition_map
                    .insert(global_idx as usize, (cluster_id as u32, local_idx as u32));
            }

            let station = &db_stations[global_idx as usize];
            let is_border = border_stops.contains(&(global_idx as usize));
            let is_hub = hubs.contains(&(global_idx as usize))
                || border_stops.contains(&(global_idx as usize));

            // Find mapped GTFS stops
            let mapped_stop_indices = station_idx_to_stop_indices
                .get(&(global_idx as usize))
                .cloned()
                .unwrap_or_default();
            let gtfs_stop_ids: Vec<String> = mapped_stop_indices
                .iter()
                .map(|&i| db_stops[i].gtfs_id.clone())
                .collect();

            // Determine Chateau (use first mapped stop)
            let chateau = if let Some(&first_stop_idx) = mapped_stop_indices.get(0) {
                &db_stops[first_stop_idx].chateau
            } else {
                "unknown" // Should not happen
            };

            let t_stop = TransitStop {
                id: local_idx as u64,
                chateau_idx: get_chateau_idx(chateau),
                station_id: station.station_id.clone(),
                gtfs_stop_ids,
                is_hub,
                is_border,
                is_external_gateway: false,
                is_long_distance: false,
                lat: station.point.y,
                lon: station.point.x,
            };
            chunk_stops.push(t_stop);

            if let Some(link) = find_osm_link(
                station.point.x,
                station.point.y,
                local_idx as u32,
                &args.osm_chunks,
                &mut chunk_cache,
            )
            .await
            {
                osm_links.push(link);
            }
        }

        // Inter-Chateau Transfers
        // Find stops from OTHER chateaus that are close to stops in this chunk.
        // Optimization: We query the DB for all stops in the BBox of this cluster.
        let mut external_transfers = Vec::new();

        // Calculate Cluster BBox
        let mut min_lat = f64::MAX;
        let mut max_lat = f64::MIN;
        let mut min_lon = f64::MAX;
        let mut max_lon = f64::MIN;
        for &idx in cluster_stop_indices {
            let s = &db_stops[idx];
            let lat = s.point.as_ref().map(|p| p.y).unwrap_or(0.0);
            let lon = s.point.as_ref().map(|p| p.x).unwrap_or(0.0);
            if lat < min_lat {
                min_lat = lat;
            }
            if lat > max_lat {
                max_lat = lat;
            }
            if lon < min_lon {
                min_lon = lon;
            }
            if lon > max_lon {
                max_lon = lon;
            }
        }

        // Calculate Convex Hull
        let points: Vec<Point<f64>> = cluster_stop_indices
            .iter()
            .map(|&idx| {
                let s = &db_stops[idx];
                let lat = s.point.as_ref().map(|p| p.y).unwrap_or(0.0);
                let lon = s.point.as_ref().map(|p| p.x).unwrap_or(0.0);
                Point::new(lon, lat) // Geo uses (x, y) = (lon, lat)
            })
            .collect();

        let hull = MultiPoint(points).concave_hull(2.0);
        let boundary_points: Vec<BoundaryPoint> = hull
            .exterior()
            .points()
            .map(|p| BoundaryPoint {
                lat: p.y(),
                lon: p.x(),
            })
            .collect();

        let boundary = PartitionBoundary {
            points: boundary_points,
        };
        partition_boundaries.insert(cluster_id_to_partition_id[cluster_id], boundary.clone());

        // Expand BBox by ~1000m (approx 0.01 deg)
        let expansion = 0.01;
        let search_min_lat = min_lat - expansion;
        let search_max_lat = max_lat + expansion;
        let search_min_lon = min_lon - expansion;
        let search_max_lon = max_lon + expansion;

        // Query DB for external stops
        let chateaux_vec: Vec<String> = chateaux_to_process.iter().cloned().collect();
        let external_stops = fetch_stops_in_bbox(
            search_min_lat,
            search_max_lat,
            search_min_lon,
            search_max_lon,
            &chateaux_vec,
            &mut conn,
        )
        .await?;

        println!("  - Found {} external stops in BBox", external_stops.len());

        // Precompute is_train_stop for all local stops
        // Map: local_idx -> bool
        let mut is_train_stop = vec![false; chunk_stops.len()];
        for pat in &relevant_patterns {
            let is_train_route =
                if let Some(route) = route_map.get(&(pat.chateau.clone(), pat.route_id.clone())) {
                    route.route_type == 1 || route.route_type == 2
                } else {
                    false
                };

            if is_train_route {
                for &global_idx in &pat.stop_indices {
                    if let Some(&local_idx) = local_stop_map.get(&global_idx) {
                        is_train_stop[local_idx as usize] = true;
                    }
                }
            }
        }

        // Build RTree of local stops
        let local_points: Vec<GeomWithData<[f64; 2], usize>> = chunk_stops
            .iter()
            .enumerate()
            .map(|(i, s)| GeomWithData::new([s.lon, s.lat], i))
            .collect();
        let local_rtree = RTree::bulk_load(local_points);

        for ext_stop in external_stops {
            let ext_lat = ext_stop.point.as_ref().map(|p| p.y).unwrap_or(0.0);
            let ext_lon = ext_stop.point.as_ref().map(|p| p.x).unwrap_or(0.0);

            // Find nearest local stops (within 3km for cycling)
            // Use RTree for efficient query
            // locate_within_distance uses squared euclidean distance for performance if using simple points,
            // but for GeomWithData it might be different.
            // Actually, rstar works in the coordinate space provided.
            // Since we are using lat/lon, "distance" is in degrees.
            // 3km is roughly 0.027 degrees (very approx).
            // To be safe and accurate, we should query a larger box or use a custom distance function if supported,
            // but locate_within_distance expects the metric of the space.
            // A simple approach is to query a bounding box or a slightly larger radius in degrees,
            // then filter by exact haversine distance.
            // 3km ~ 0.03 degrees.

            let search_radius_deg = 0.04; // Conservative upper bound
            let candidates = local_rtree
                .locate_within_distance([ext_lon, ext_lat], search_radius_deg * search_radius_deg);
            // Wait, locate_within_distance for [f64; 2] uses squared euclidean distance.

            for candidate in candidates {
                let local_idx = candidate.data;
                let stop = &mut chunk_stops[local_idx];

                let dist = haversine_distance(stop.lat, stop.lon, ext_lat, ext_lon);
                if dist <= 3000.0 {
                    // Create transfer
                    let walk_seconds = (dist / 1.4) as u32; // ~1.4 m/s walking speed

                    // Check accessibility (1 = some accessibility, 2 = none, 0 = unknown)
                    // We consider 1 as accessible.
                    let is_accessible = ext_stop.wheelchair_boarding == 1;

                    external_transfers.push(ExternalTransfer {
                        from_stop_idx: stop.id as u32, // stop.id is local_idx as u64
                        to_chateau_idx: get_chateau_idx(&ext_stop.chateau),
                        to_stop_gtfs_id: ext_stop.gtfs_id.clone(),
                        walk_seconds,
                        distance_meters: dist as u32,
                        wheelchair_accessible: is_accessible,
                    });

                    // Mark as External Gateway
                    // If dist <= 200m, it's a gateway.
                    // If dist <= 300m AND it's a train station, it's a gateway.

                    let is_train = is_train_stop[local_idx];
                    let threshold = if is_train { 400.0 } else { 300.0 };

                    if dist <= threshold {
                        stop.is_external_gateway = true;
                    }
                }
            }
        }

        let mut chunk_patterns = Vec::new();
        let mut direction_patterns = Vec::new();
        let mut direction_pattern_map: HashMap<Vec<u32>, u32> = HashMap::new();

        for pat in relevant_patterns {
            let local_indices: Vec<u32> = pat
                .stop_indices
                .iter()
                .map(|idx| *local_stop_map.get(idx).unwrap() as u32)
                .collect();

            let dp_idx = if let Some(&idx) = direction_pattern_map.get(&local_indices) {
                idx
            } else {
                let idx = direction_patterns.len() as u32;
                direction_patterns.push(DirectionPattern {
                    stop_indices: local_indices.clone(),
                });
                direction_pattern_map.insert(local_indices, idx);
                idx
            };

            chunk_patterns.push(TripPattern {
                chateau_idx: get_chateau_idx(&pat.chateau),
                route_id: pat.route_id.clone(),
                direction_pattern_idx: dp_idx,
                trips: pat.trips.clone(),
                timezone_idx: pat.timezone_idx,
            });
        }

        let (local_deltas, reindexed_patterns) =
            reindex_deltas(chunk_patterns, &global_time_deltas, &direction_patterns);

        let partition_id = cluster_id_to_partition_id[cluster_id];

        let mut partition = TransitPartition {
            partition_id,
            stops: chunk_stops,
            trip_patterns: reindexed_patterns,
            time_deltas: local_deltas,
            direction_patterns,
            internal_transfers: Vec::new(),
            osm_links,
            service_ids: service_ids.clone(),
            service_exceptions: Vec::new(),
            _deprecated_external_transfers: Vec::new(),
            local_dag: std::collections::HashMap::new(),
            long_distance_trip_patterns: Vec::new(),
            timezones: global_timezones.clone(),
            boundary: Some(boundary),
            chateau_ids: chateau_ids.clone(),
            external_hubs: Vec::new(),
            long_distance_transfer_patterns: Vec::new(),
        };

        // Save Transfer Chunk
        let transfer_chunk = TransferChunk {
            partition_id,
            external_transfers,
        };
        let transfer_filename = format!("transfers_chunk_{}.bincode", partition_id);
        let transfer_path = args.output.join(transfer_filename);
        println!("Saving transfer chunk to {}", transfer_path.display());
        save_bincode(&transfer_chunk, transfer_path.to_str().unwrap())?;

        // Collect global nodes (Hubs + Border Nodes) for global graph
        for stop in &partition.stops {
            if stop.is_hub || stop.is_border {
                all_global_nodes
                    .entry(partition.partition_id)
                    .or_default()
                    .push(stop.clone());
            }
        }

        // Compute Local Transfer Patterns
        // This populates partition.local_transfer_patterns
        println!(
            "Computing local transfer patterns for partition {}",
            partition_id
        );
        let start_timer_local_patterns = Instant::now();
        compute_local_patterns_for_partition(&mut partition);
        println!(
            "Computed local transfer patterns for partition {}, took {} ms",
            partition_id,
            start_timer_local_patterns.elapsed().as_millis()
        );

        println!("Resolve cross partition edges");

        // Resolve Cross-Partition Edges originating from this partition
        for &((u, v), ref route_id) in &cross_partition_signatures {
            if stop_to_cluster[u] == cluster_id {
                // This edge starts in this partition.
                // We need to find the pattern in this partition that connects u -> v.
                if let (Some(&local_u), Some(&local_v)) = (
                    local_stop_map.get(&(u as u32)),
                    local_stop_map.get(&(v as u32)),
                ) {
                    // Find pattern
                    for (p_idx, pattern) in partition.trip_patterns.iter().enumerate() {
                        if partition.chateau_ids[pattern.chateau_idx as usize] == route_id.0
                            && pattern.route_id == route_id.1
                        {
                            let dp = &partition.direction_patterns
                                [pattern.direction_pattern_idx as usize];

                            for i in 0..dp.stop_indices.len() - 1 {
                                if dp.stop_indices[i] == local_u as u32
                                    && dp.stop_indices[i + 1] == local_v as u32
                                {
                                    // Found it!
                                    // Calculate min_duration for segment i -> i+1
                                    let mut min_duration = u32::MAX;
                                    for trip in &pattern.trips {
                                        let delta_seq =
                                            &partition.time_deltas[trip.time_delta_idx as usize];
                                        // Travel time from i to i+1 is at index 2*(i+1)
                                        if 2 * (i + 1) < delta_seq.deltas.len() {
                                            let dur = delta_seq.deltas[2 * (i + 1)];
                                            if dur < min_duration {
                                                min_duration = dur;
                                            }
                                        }
                                    }
                                    if min_duration == u32::MAX {
                                        min_duration = 0;
                                    }

                                    let transit_edge = TransitEdge {
                                        trip_pattern_idx: p_idx as u32,
                                        start_stop_idx: i as u32,
                                        end_stop_idx: (i + 1) as u32,
                                        min_duration,
                                    };
                                    cross_partition_edges.push((
                                        (u, v),
                                        DagEdge {
                                            from_node_idx: u as u32,
                                            to_node_idx: v as u32,
                                            edge_type: Some(EdgeType::Transit(transit_edge)),
                                        },
                                    ));
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }

        println!(
            "Partition {} has {} stops, {} trip_patterns, {} time_deltas, {} direction patterns, {} internal transfers, {} osm links, {} local transfer patterns, {} long distance patterns",
            partition_id,
            partition.stops.len(),
            partition.trip_patterns.len(),
            partition.time_deltas.len(),
            partition.direction_patterns.len(),
            partition.internal_transfers.len(),
            partition.osm_links.len(),
            partition.local_dag.len(),
            partition.long_distance_trip_patterns.len()
        );

        partitions.insert(partition_id, partition.clone());

        let filename = format!("transit_chunk_{}.bincode", partition_id);
        let path = args.output.join(filename);

        // Debug: Print approximate sizes
        {
            let stops_size: usize = partition
                .stops
                .iter()
                .map(|s| std::mem::size_of::<TransitStop>() + s.station_id.len())
                .sum();
            let trip_patterns_size: usize = partition
                .trip_patterns
                .iter()
                .map(|tp| {
                    std::mem::size_of::<TripPattern>()
                        + tp.route_id.len()
                        + tp.trips
                            .iter()
                            .map(|t| std::mem::size_of::<CompressedTrip>() + t.gtfs_trip_id.len())
                            .sum::<usize>()
                })
                .sum();
            let time_deltas_size: usize = partition
                .time_deltas
                .iter()
                .map(|td| std::mem::size_of::<TimeDeltaSequence>() + td.deltas.len() * 4)
                .sum();
            let internal_transfers_size: usize =
                partition.internal_transfers.len() * std::mem::size_of::<StaticTransfer>();
            let direction_patterns_size: usize = partition
                .direction_patterns
                .iter()
                .map(|dp| std::mem::size_of::<DirectionPattern>() + dp.stop_indices.len() * 4)
                .sum();
            let local_dag_size: usize = partition
                .local_dag
                .iter()
                .map(|(k, v)| {
                    std::mem::size_of::<u32>()
                        + std::mem::size_of::<catenary::routing_common::transit_graph::DagEdgeList>(
                        )
                        + v.edges.len()
                            * std::mem::size_of::<catenary::routing_common::transit_graph::DagEdge>(
                            )
                })
                .sum();
            let long_distance_trip_patterns_size: usize = partition
                .long_distance_trip_patterns
                .iter()
                .map(|tp| {
                    std::mem::size_of::<TripPattern>()
                        + tp.route_id.len()
                        + tp.trips
                            .iter()
                            .map(|t| std::mem::size_of::<CompressedTrip>() + t.gtfs_trip_id.len())
                            .sum::<usize>()
                })
                .sum();

            println!(
                "Partition {} sizes (approx bytes): Stops: {}, TripPatterns: {}, TimeDeltas: {}, InternalTransfers: {}, DirectionPatterns: {}, LocalTransferPatterns: {}, LongDistanceTripPatterns: {}",
                partition.partition_id,
                stops_size,
                trip_patterns_size,
                time_deltas_size,
                internal_transfers_size,
                direction_patterns_size,
                local_dag_size,
                long_distance_trip_patterns_size
            );
        }

        // Saving moved to end of function
        println!("Saving transit chunk to {}", path.display());
        save_bincode(&partition, path.to_str().unwrap())?;
    }

    // 6. Transfer Pattern Precomputation (Stubs)
    // NEW: Save Manifest and Edges
    println!("Saving Manifest and Edge Files...");

    // Manifest
    let mut chateau_to_partitions: HashMap<String, Vec<u32>> = HashMap::new();
    let mut partition_to_chateaux: HashMap<u32, Vec<String>> = HashMap::new();

    for (s_idx, stop) in db_stops.iter().enumerate() {
        let pid = cluster_id_to_partition_id[stop_to_cluster[s_idx]];
        chateau_to_partitions
            .entry(stop.chateau.clone())
            .or_default()
            .push(pid);
        partition_to_chateaux
            .entry(pid)
            .or_default()
            .push(stop.chateau.clone());
    }

    // Merge with existing manifest (excluding removed partitions)
    for (chateau, pids) in existing_manifest.chateau_to_partitions {
        if !chateaux_to_process.contains(&chateau) {
            let entry = chateau_to_partitions.entry(chateau).or_default();
            for pid in pids {
                if !partitions_to_remove.contains(&pid) {
                    entry.push(pid);
                }
            }
        }
    }

    for (pid, chateaux) in existing_manifest.partition_to_chateaux {
        if !partitions_to_remove.contains(&pid) {
            let entry = partition_to_chateaux.entry(pid).or_default();
            for c in chateaux {
                entry.push(c);
            }
        }
    }

    for (pid, boundary) in existing_manifest.partition_boundaries {
        if !partitions_to_remove.contains(&pid) {
            partition_boundaries.insert(pid, boundary);
        }
    }

    // Clean up duplicates again
    for v in chateau_to_partitions.values_mut() {
        v.sort();
        v.dedup();
    }
    for v in partition_to_chateaux.values_mut() {
        v.sort();
        v.dedup();
    }

    let manifest = Manifest {
        chateau_to_partitions: chateau_to_partitions.into_iter().collect(),
        partition_to_chateaux: partition_to_chateaux.into_iter().collect(),
        partition_boundaries: partition_boundaries.into_iter().collect(),
    };

    let manifest_file = File::create(args.output.join("manifest.json"))?;
    serde_json::to_writer(manifest_file, &manifest)?;

    // Cleanup Stale Partitions
    println!("Cleaning up stale partitions...");
    let valid_partitions: HashSet<u32> = manifest.partition_to_chateaux.keys().cloned().collect();
    let mut entries = tokio::fs::read_dir(&args.output).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if let Some(filename) = path.file_name().and_then(|f| f.to_str()) {
            let pid_opt = if filename.starts_with("transit_chunk_")
                && filename.ends_with(".bincode")
            {
                filename
                    .trim_start_matches("transit_chunk_")
                    .trim_end_matches(".bincode")
                    .parse::<u32>()
                    .ok()
            } else if filename.starts_with("transfers_chunk_") && filename.ends_with(".bincode") {
                filename
                    .trim_start_matches("transfers_chunk_")
                    .trim_end_matches(".bincode")
                    .parse::<u32>()
                    .ok()
            } else {
                None
            };

            if let Some(pid) = pid_opt {
                if !valid_partitions.contains(&pid) {
                    println!("Deleting stale partition file: {}", filename);
                    tokio::fs::remove_file(path).await?;
                }
            }
        }
    }

    // Edges
    let mut edges_by_partition: HashMap<u32, Vec<EdgeEntry>> = HashMap::new();

    for &((u, v), ref sig) in &cross_partition_edges {
        let p_from = cluster_id_to_partition_id[stop_to_cluster[u]];
        let s_from = &db_stops[u];
        let s_to = &db_stops[v];

        edges_by_partition
            .entry(p_from)
            .or_default()
            .push(EdgeEntry {
                from_chateau: s_from.chateau.clone(),
                from_id: s_from.gtfs_id.clone(),
                to_chateau: s_to.chateau.clone(),
                to_id: s_to.gtfs_id.clone(),
                edge_type: sig.edge_type.clone(),
            });
    }

    for (pid, edges) in edges_by_partition {
        let path = args.output.join(format!("edges_chunk_{}.json", pid));
        let file = File::create(path)?;
        serde_json::to_writer(file, &edges)?;
    }

    // 6. Transfer Pattern Precomputation
    println!("Computing Transfer Patterns...");
    // Local patterns computed per partition above.
    compute_global_patterns(
        &all_global_nodes,
        &cross_partition_edges,
        &global_to_partition_map,
        &mut partitions,
        &args.output,
    );

    // 6b. Compute Border Patterns (Step 4)
    let partition_dags =
        compute_border_patterns(&all_global_nodes, &partitions, &global_to_partition_map);

    // Save Global Pattern Index
    let global_index = GlobalPatternIndex {
        partition_dags,
        long_distance_dags: Vec::new(),
    };
    let global_path = args.output.join("global_patterns.bincode");
    if let Err(e) = save_bincode(&global_index, global_path.to_str().unwrap()) {
        eprintln!("Failed to save global_patterns.bincode: {}", e);
    } else {
        println!("  - Saved global_patterns.bincode");
    }

    // 6c. Save Timetable Data (Per Chateau)
    println!("Saving Timetable Data per Chateau...");

    let mut chateau_partitions: HashMap<String, Vec<u32>> = HashMap::new();
    for (pid, partition) in &partitions {
        for chateau in &partition.chateau_ids {
            chateau_partitions
                .entry(chateau.clone())
                .or_default()
                .push(*pid);
        }
    }

    for (chateau_id, pids) in chateau_partitions {
        let mut partition_timetables = Vec::new();
        for pid in pids {
            if let Some(partition) = partitions.get(&pid) {
                partition_timetables.push(PartitionTimetableData {
                    partition_id: pid,
                    trip_patterns: partition.trip_patterns.clone(),
                    time_deltas: partition.time_deltas.clone(),
                    service_ids: partition.service_ids.clone(),
                    service_exceptions: partition.service_exceptions.clone(),
                    timezones: partition.timezones.clone(),
                    direction_patterns: partition.direction_patterns.clone(),
                });
            }
        }

        let timetable_data = TimetableData {
            chateau_id: chateau_id.clone(),
            partitions: partition_timetables,
        };

        let filename = format!("timetable_data_{}.bincode", chateau_id);
        let path = args.output.join(filename);
        if let Err(e) = save_bincode(&timetable_data, path.to_str().unwrap()) {
            eprintln!("Failed to save timetable data for {}: {}", chateau_id, e);
        } else {
            println!("  - Saved timetable data for {} to {:?}", chateau_id, path);
        }
    }

    // 7. Save Final Partitions (with Global Patterns)
    println!("Saving Final Partitions...");
    for (pid, partition) in partitions {
        let path = args.output.join(format!("transit_chunk_{}.bincode", pid));
        if let Err(e) = save_bincode(&partition, path.to_str().unwrap()) {
            eprintln!("Failed to save partition {}: {}", pid, e);
        } else {
            println!("  - Saved partition {} to {:?}", pid, path);
        }
    }

    Ok(())
}

// --- Geometry Helpers ---

// --- Quality Check Logic ---

fn check_partition_quality(clusters: &[Vec<usize>], stops: &[Stop]) -> bool {
    if clusters.is_empty() {
        return true;
    }

    // 1. Balance Check
    let mut min_size = usize::MAX;
    let mut max_size = 0;
    for c in clusters {
        let size = c.len();
        if size < min_size {
            min_size = size;
        }
        if size > max_size {
            max_size = size;
        }
    }

    if min_size > 0 && (max_size as f64 / min_size as f64) > 5.0 {
        // println!("  ! Balance check failed: Max/Min ratio = {:.2}", max_size as f64 / min_size as f64);
        return false;
    }

    // Concavity check removed as per user request (concavity has nothing to do with physical geometry)

    true
}
fn recluster_without_weights(
    stop_indices: &[usize],
    original_adjacency: &HashMap<(usize, usize), u32>,
    _stops: &[Stop],
    max_size: usize,
) -> Vec<Vec<usize>> {
    // 1. Map Global <-> Local
    let mut global_to_local = HashMap::new();
    let mut local_to_global = Vec::new();
    for (i, &g_idx) in stop_indices.iter().enumerate() {
        global_to_local.insert(g_idx, i);
        local_to_global.push(g_idx);
    }

    // 2. Build subgraph adjacency with *original* weights
    let mut sub_adjacency: HashMap<(usize, usize), u32> = HashMap::new();
    for (&(u, v), &w) in original_adjacency {
        if let (Some(&lu), Some(&lv)) = (global_to_local.get(&u), global_to_local.get(&v)) {
            let (min, max) = if lu < lv { (lu, lv) } else { (lv, lu) };
            *sub_adjacency.entry((min, max)).or_default() += w;
        }
    }

    // 3. Run clustering
    let local_clusters = merge_based_clustering(stop_indices.len(), &sub_adjacency, max_size);

    // 4. Map back
    local_clusters
        .into_iter()
        .map(|c| c.into_iter().map(|l_idx| local_to_global[l_idx]).collect())
        .collect()
}

async fn fetch_stops_in_bbox(
    min_lat: f64,
    max_lat: f64,
    min_lon: f64,
    max_lon: f64,
    excluded_chateaux: &[String],
    conn: &mut AsyncPgConnection,
) -> Result<Vec<Stop>> {
    use catenary::schema::gtfs::stops::dsl::*;
    use diesel::dsl::sql;
    use diesel::sql_types::Bool;

    // ST_MakeEnvelope(xmin, ymin, xmax, ymax, srid)
    let raw_sql = format!(
        "stops.point && ST_MakeEnvelope({}, {}, {}, {}, 4326)",
        min_lon, min_lat, max_lon, max_lat
    );

    let results = stops
        .filter(chateau.ne_all(excluded_chateaux))
        .filter(sql::<Bool>(&raw_sql))
        .select(Stop::as_select())
        .load(conn)
        .await?;

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clustering_orphans() {
        // Scenario:
        // Cluster A: Nodes 0..100 (Size 100) - Fully connected clique
        // Cluster B: Node 100 (Size 1) - Connected to Node 0
        // Max Size: 100
        //
        // Pass 1: Should merge 0..99 into one cluster (Size 100).
        //         Node 100 cannot merge because 100 + 1 > 100.
        // Pass 2: Node 100 is orphan (Size 1 < 20). Should force merge into Cluster A.

        let mut adjacency = HashMap::new();

        // Create clique 0..99
        for i in 0..100 {
            for j in (i + 1)..100 {
                adjacency.insert((i, j), 1);
            }
        }

        // Connect 100 to 0
        adjacency.insert((0, 100), 10); // Strong connection

        let clusters = merge_based_clustering(101, &adjacency, 100);

        // Expect 1 cluster
        assert_eq!(
            clusters.len(),
            1,
            "Should have merged orphan into main cluster"
        );
        assert_eq!(clusters[0].len(), 101, "Cluster should contain all nodes");
    }

    #[test]
    fn test_clustering_basic() {
        // Simple case: 4 nodes, 0-1, 2-3. Max size 2.
        // Should result in 2 clusters.
        let mut adjacency = HashMap::new();
        adjacency.insert((0, 1), 1);
        adjacency.insert((2, 3), 1);

        let clusters = merge_based_clustering(4, &adjacency, 2);
        assert_eq!(clusters.len(), 2);
    }

    #[test]
    fn test_hub_identification() {
        // Mock Stops
        let mut stops = Vec::new();
        for i in 0..10 {
            stops.push(Stop {
                onestop_feed_id: "test".to_string(),
                attempt_id: "test".to_string(),
                gtfs_id: i.to_string(),
                name: Some(format!("Stop {}", i)),
                name_translations: None,
                displayname: None,
                code: None,
                gtfs_desc: None,
                gtfs_desc_translations: None,
                location_type: 0,
                parent_station: None,
                zone_id: None,
                url: None,
                point: Some(postgis_diesel::types::Point {
                    x: i as f64 * 0.01,
                    y: 0.0,
                    srid: Some(4326),
                }),
                timezone: None,
                wheelchair_boarding: 0,
                primary_route_type: None,
                level_id: None,
                platform_code: None,
                platform_code_translations: None,
                routes: vec![],
                route_types: vec![],
                children_ids: vec![],
                children_route_types: vec![],
                station_feature: false,
                hidden: false,
                chateau: "test".to_string(),
                location_alias: None,
                tts_name_translations: None,
                tts_name: None,
                allowed_spatial_query: true,
            });
        }

        // Mock Patterns
        // Pattern 1: 0 -> 1 -> 2 -> 3 -> 4 (Line A)
        // Pattern 2: 5 -> 6 -> 2 -> 7 -> 8 (Line B) - Intersects at 2

        let p1 = ProcessedPattern {
            chateau: "test".to_string(),
            route_id: "A".to_string(),
            stop_indices: vec![0, 1, 2, 3, 4],
            trips: vec![CompressedTrip {
                gtfs_trip_id: "t1".to_string(),
                service_mask: 127,
                start_time: 28800, // 8:00 AM
                time_delta_idx: 0,
                service_idx: 0,
                bikes_allowed: 0,
                wheelchair_accessible: 0,
            }],
            timezone_idx: 0,
        };

        let p2 = ProcessedPattern {
            chateau: "test".to_string(),
            route_id: "B".to_string(),
            stop_indices: vec![5, 6, 2, 7, 8],
            trips: vec![CompressedTrip {
                gtfs_trip_id: "t2".to_string(),
                service_mask: 127,
                start_time: 29100, // 8:05 AM
                time_delta_idx: 0, // Offset
                service_idx: 0,
                bikes_allowed: 0,
                wheelchair_accessible: 0,
            }],
            timezone_idx: 0,
        };

        let patterns = vec![p1, p2];

        // Time deltas: [Travel0, Dwell0, Travel1, Dwell1, Travel2, Dwell2]
        // Stop 0: Travel=0, Dwell=0
        // Stop 1: Travel=10min (600), Dwell=0
        // Stop 2: Travel=10min (600), Dwell=0
        let time_deltas = vec![TimeDeltaSequence {
            deltas: vec![0, 0, 600, 0, 600, 0],
        }];

        let calendar = vec![];

        // Run with sample_size = 100
        // We need to make sure sample picks random nodes that force transfer at 2.
        // With 10 nodes, 100 queries should hit 0->8 or 5->4 etc.
        let hubs =
            identify_hubs_time_independent(&stops, &patterns, &time_deltas, &calendar, 500, 1);

        // We expect 2 to be the hub
        assert!(hubs.contains(&2), "Node 2 should be identified as a hub");
    }

    #[test]
    fn test_hub_loop_penalty() {
        // Scenario:
        // Pattern 1 (Loop): 0 -> 1 -> 2 -> 3 -> 0 (Route A, Pattern Idx 0)
        // We force a transfer from 0->1 to 3->0 by having a "Walk" between 1 and 3?
        // Or better:
        // Pattern 1: 0 -> 1 -> 2 (Eastbound)
        // Pattern 1 (same pat? No, usually loop is one long pattern): 0 -> 1 -> 2 -> 3 -> 4 -> 5 (where 5 is near 0)
        // Let's simulate:
        // Stop 0 and Stop 10 are close (Walkable).
        // Pattern A: 0 -> 1 -> ... -> 9 -> 10.
        // Trip goes 0->10.
        // If we go 0->1, walk 1->9 (shortcut?), take 9->10.
        // That's a self-transfer on Pattern A.
        // Nodes 1 and 9 should NOT be hubs.

        // Scenario 2:
        // Pattern B: 20 -> 21.
        // Pattern C: 21 -> 22.
        // Transfer at 21.
        // Node 21 SHOULD be a hub.

        let mut stops = Vec::new();
        for i in 0..15 {
            stops.push(Stop {
                onestop_feed_id: "test".to_string(),
                attempt_id: "test".to_string(),
                gtfs_id: i.to_string(),
                name: Some(format!("Stop {}", i)),
                name_translations: None,
                displayname: None,
                code: None,
                gtfs_desc: None,
                gtfs_desc_translations: None,
                location_type: 0,
                parent_station: None,
                zone_id: None,
                url: None,
                point: Some(postgis_diesel::types::Point {
                    x: 0.0,
                    y: 0.0,
                    srid: Some(4326),
                }),
                timezone: None,
                wheelchair_boarding: 0,
                primary_route_type: None,
                level_id: None,
                platform_code: None,
                platform_code_translations: None,
                routes: vec![],
                route_types: vec![],
                children_ids: vec![],
                children_route_types: vec![],
                station_feature: false,
                hidden: false,
                chateau: "test".to_string(),
                location_alias: None,
                tts_name_translations: None,
                tts_name: None,
                allowed_spatial_query: true,
            });
        }

        // Geometry Setup:
        // 0 and 10 are at (0, 0).
        // 1 is at (0, 1.0). Far from 0.
        // 9 is at (0.001, 1.0). Close to 1 (~100m).
        // Intermediate stops 2..8 are far away (e.g. 0, 2.0).

        stops[0].point = Some(postgis_diesel::types::Point {
            x: 0.0,
            y: 0.0,
            srid: Some(4326),
        });
        stops[10].point = Some(postgis_diesel::types::Point {
            x: 0.0,
            y: 0.0,
            srid: Some(4326),
        });

        stops[1].point = Some(postgis_diesel::types::Point {
            x: 0.0,
            y: 1.0,
            srid: Some(4326),
        });
        stops[9].point = Some(postgis_diesel::types::Point {
            x: 0.001,
            y: 1.0,
            srid: Some(4326),
        });

        // Move others away so they don't interfere and don't form a clique
        for i in 3..15 {
            if i == 11 || i == 12 || i == 13 {
                continue;
            }
            stops[i].point = Some(postgis_diesel::types::Point {
                x: 100.0 + i as f64,
                y: 100.0 + i as f64,
                srid: Some(4326),
            });
        }

        // 11, 12, 13 for Route B/C
        stops[11].point = Some(postgis_diesel::types::Point {
            x: 10.0,
            y: 0.0,
            srid: Some(4326),
        });
        stops[12].point = Some(postgis_diesel::types::Point {
            x: 10.1,
            y: 0.0,
            srid: Some(4326),
        }); // Far from others
        stops[13].point = Some(postgis_diesel::types::Point {
            x: 10.2,
            y: 0.0,
            srid: Some(4326),
        });

        let p_loop = ProcessedPattern {
            chateau: "test".to_string(),
            route_id: "LoopRoute".to_string(),
            stop_indices: vec![0, 1, 2],
            trips: vec![CompressedTrip {
                gtfs_trip_id: "t1".to_string(),
                service_mask: 127,
                start_time: 28000,
                time_delta_idx: 0,
                service_idx: 0,
                bikes_allowed: 0,
                wheelchair_accessible: 0,
            }],
            timezone_idx: 0,
        };

        let p_b = ProcessedPattern {
            chateau: "test".to_string(),
            route_id: "RouteB".to_string(),
            stop_indices: vec![11, 12],
            trips: (0..20)
                .map(|i| CompressedTrip {
                    gtfs_trip_id: format!("tb_{}", i),
                    service_mask: 127,
                    start_time: 25200 + i * 600,
                    time_delta_idx: 0,
                    service_idx: 0,
                    bikes_allowed: 0,
                    wheelchair_accessible: 0,
                })
                .collect(),
            timezone_idx: 0,
        };

        let p_c = ProcessedPattern {
            chateau: "test".to_string(),
            route_id: "RouteC".to_string(),
            stop_indices: vec![12, 13],
            trips: (0..20)
                .map(|i| CompressedTrip {
                    gtfs_trip_id: format!("tc_{}", i),
                    service_mask: 127,
                    start_time: 25200 + i * 600 + 300, // Offset by 5 mins to allow transfer
                    time_delta_idx: 0,
                    service_idx: 0,
                    bikes_allowed: 0,
                    wheelchair_accessible: 0,
                })
                .collect(),
            timezone_idx: 0,
        };

        let patterns = vec![p_loop, p_b, p_c];

        // Let's adjust time deltas.
        // We need `reindex_deltas` to work? `identify_hubs` builds its own partition.
        // It uses `time_deltas` passed in.
        // p_loop uses delta_pointer 0.
        // indices: 0, 1, 2...
        // 0->1: delta[1].
        // 1->2: delta[2].

        let mut deltas = vec![0; 100];
        let time_deltas = vec![TimeDeltaSequence { deltas }];

        let hubs =
            identify_hubs_time_independent(&stops, &patterns, &time_deltas, &[], stops.len(), 5);
        println!("  - Identified Hubs: {:?}", hubs);

        // 12 should be a hub (Transfer B->C).
        assert!(
            hubs.contains(&12),
            "Node 12 should be a hub (Inter-route transfer)"
        );
    }

    #[test]
    fn test_dag_formation() {
        // Setup:
        // Partition 0: Node 0 (Border)
        // Partition 1: Node 1 (Border)
        // Cross Edge: 0 -> 1
        // Expected: DAG for 0 -> 1 containing 0, 1 and edge 0->1.

        let temp_dir = std::env::temp_dir().join("gentian_test_dag");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let mut border_nodes = HashMap::new();
        // We don't really use the value of border_nodes in compute_global_patterns except for keys?
        // Wait, compute_global_patterns uses `border_nodes.keys()` to get partition IDs.
        border_nodes.insert(0, vec![]);
        border_nodes.insert(1, vec![]);

        let mut cross_edges = Vec::new();
        cross_edges.push((
            (0, 1),
            DagEdge {
                from_node_idx: 0,
                to_node_idx: 1,
                edge_type: Some(EdgeType::Walk(WalkEdge {
                    duration_seconds: 100,
                })),
            },
        ));

        let mut global_to_partition_map = HashMap::new();
        global_to_partition_map.insert(0, (0, 0)); // Node 0 is in P0, local idx 0
        global_to_partition_map.insert(1, (1, 0)); // Node 1 is in P1, local idx 0

        let mut loaded_partitions = HashMap::new();

        // Partition 0
        // Partition 0
        let mut p0 = TransitPartition::default();
        p0.partition_id = 0;
        p0.stops.push(TransitStop {
            id: 0,
            chateau_idx: 0,
            station_id: "s0".to_string(),
            gtfs_stop_ids: vec!["s0".to_string()],
            is_hub: true, // Must be hub to be considered long-distance
            is_border: true,
            is_external_gateway: false,
            is_long_distance: true,
            lat: 0.0,
            lon: 0.0,
        });
        // Add dummy stop for long distance
        p0.stops.push(TransitStop {
            id: 1,
            chateau_idx: 0,
            station_id: "s0_dummy".to_string(),
            gtfs_stop_ids: vec!["s0_dummy".to_string()],
            is_hub: false,
            is_border: false,
            is_external_gateway: false,
            is_long_distance: false,
            lat: 1.0, // ~111km away
            lon: 0.0,
        });
        p0.direction_patterns.push(DirectionPattern {
            stop_indices: vec![0, 1],
        });
        p0.trip_patterns.push(TripPattern {
            chateau_idx: 0,
            route_id: "r0".to_string(),
            direction_pattern_idx: 0,
            trips: vec![],
            timezone_idx: 0,
        });
        loaded_partitions.insert(0, p0);

        // Partition 1
        let mut p1 = TransitPartition::default();
        p1.partition_id = 1;
        p1.stops.push(TransitStop {
            id: 0,
            chateau_idx: 0,
            station_id: "s1".to_string(),
            gtfs_stop_ids: vec!["s1".to_string()],
            is_hub: true,
            is_border: true,
            is_external_gateway: false,
            is_long_distance: true,
            lat: 0.0,
            lon: 0.0,
        });
        // Add dummy stop for long distance
        p1.stops.push(TransitStop {
            id: 1,
            chateau_idx: 0,
            station_id: "s1_dummy".to_string(),
            gtfs_stop_ids: vec!["s1_dummy".to_string()],
            is_hub: false,
            is_border: false,
            is_external_gateway: false,
            is_long_distance: false,
            lat: 1.0,
            lon: 0.0,
        });
        p1.direction_patterns.push(DirectionPattern {
            stop_indices: vec![0, 1],
        });
        p1.trip_patterns.push(TripPattern {
            chateau_idx: 0,
            route_id: "r1".to_string(),
            direction_pattern_idx: 0,
            trips: vec![],
            timezone_idx: 0,
        });
        loaded_partitions.insert(1, p1);

        compute_global_patterns(
            &border_nodes,
            &cross_edges,
            &global_to_partition_map,
            &mut loaded_partitions,
            &temp_dir,
        );

        // Verify P0
        let p0 = &loaded_partitions[&0];

        // Should have external hub 1 (from P1)
        assert_eq!(p0.external_hubs.len(), 1);
        assert_eq!(p0.external_hubs[0].original_partition_id, 1);
        assert_eq!(p0.external_hubs[0].stop_idx_in_partition, 0);

        // Should have pattern 0 -> 1
        // 0 is local index 0. 1 is external hub index 0 (mapped to stops.len() + 0).
        let ext_hub_idx = p0.stops.len() as u32;

        let pattern = p0
            .long_distance_transfer_patterns
            .iter()
            .find(|p| p.from_stop_idx == 0);
        assert!(pattern.is_some(), "Should have pattern starting at 0");

        let edges = &pattern.unwrap().edges;
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].from_node_idx, 0);
        assert_eq!(edges[0].to_node_idx, ext_hub_idx);
    }
}

fn identify_hubs_time_independent(
    stops: &[Stop],
    patterns: &[ProcessedPattern],
    time_deltas: &[TimeDeltaSequence],
    _calendar: &[Calendar],
    sample_size: usize,
    top_k: usize,
) -> HashSet<usize> {
    println!("  - Building static graph for centrality analysis...");

    // 1. Build Static Graph
    // Adjacency list: node_idx -> vec<(neighbor_idx, weight_seconds)>
    let mut adj: Vec<Vec<(usize, u32)>> = vec![Vec::new(); stops.len()];

    // Add Transit Edges (min travel time)
    for pat in patterns {
        if pat.stop_indices.is_empty() {
            continue;
        }

        // We need to find the minimum travel time between adjacent stops in this pattern across all trips.
        // This is an approximation. The paper says "overlay the nodes and arcs of each line... using the minimum of arc costs".

        // For each segment (u, v) in the pattern
        for i in 0..pat.stop_indices.len() - 1 {
            let u = pat.stop_indices[i] as usize;
            let v = pat.stop_indices[i + 1] as usize;

            // Find min duration for this segment across all trips
            let mut min_dur = u32::MAX;

            for trip in &pat.trips {
                let delta_seq = &time_deltas[trip.time_delta_idx as usize];
                // Segment i corresponds to deltas[2*i] (dwell at u) + deltas[2*i+1] (travel u->v)
                // Actually, let's check how deltas are structured.
                // In `get_arrival_time`:
                // k=0: start
                // k=1: + delta[0] (travel 0->1)
                // Wait, let's look at `get_arrival_time` again.
                // if k > 0 { time += deltas[2*k] ?? No.
                // Let's re-read `get_arrival_time` or `compute_initial_transfers`.
                // `compute_initial_transfers`:
                // i=0: time = start
                // i=1:
                //   if 2*i < len: time += deltas[2*i] (This looks like travel?)
                //   Wait, 2*i for i=1 is 2.
                //   If deltas has [travel_0_1, dwell_1, travel_1_2, dwell_2 ...]
                //   Let's assume standard GTFS deltas structure often used:
                //   Arrival[i] = Dep[i-1] + Travel[i-1->i]
                //   Dep[i] = Arrival[i] + Dwell[i]

                // In `gentian`:
                // Trip has `time_delta_idx`.
                // `get_arrival_time`:
                // for k in 0..=stop_idx
                //   if k > 0: time += deltas[2*k]  <-- This seems to be travel time to k?
                //   if k < stop_idx: time += deltas[2*k+1] <-- This seems to be dwell at k?
                // This indexing is weird.
                // Let's look at `compute_initial_transfers` line 89:
                // if i > 0 { if 2*i < len { time += deltas[2*i]; } }
                // if 2*i+1 < len { time += deltas[2*i+1]; }

                // So for segment i (from stop i to i+1):
                // We are at stop i (after arrival).
                // We add deltas[2*i+1] (Dwell at i).
                // Then for next stop (i+1), we add deltas[2*(i+1)] (Travel i->i+1).
                // So total time from Arr(i) to Arr(i+1) is Dwell(i) + Travel(i->i+1).
                // Or Dep(i) to Arr(i+1) is Travel(i->i+1).

                // We want edge weight. Usually this is travel time + dwell?
                // Or just travel time?
                // If we are routing node to node, and nodes are stops.
                // The cost to go from stop A to stop B is travel time.
                // Dwell is usually incurred at the stop.
                // Let's include both to be safe, or just travel.
                // If we include dwell, it's Dwell(u) + Travel(u->v).
                // Dwell at u is deltas[2*i + 1].
                // Travel u->v is deltas[2*(i+1)].

                let mut dur = 0;
                // Dwell at u
                if 2 * i + 1 < delta_seq.deltas.len() {
                    dur += delta_seq.deltas[2 * i + 1];
                }
                // Travel u->v
                if 2 * (i + 1) < delta_seq.deltas.len() {
                    dur += delta_seq.deltas[2 * (i + 1)];
                }

                if dur < min_dur {
                    min_dur = dur;
                }
            }

            if min_dur != u32::MAX {
                adj[u].push((v, min_dur));
            }
        }
    }

    // Add Internal Transfers (Footpaths)
    // We need to regenerate them or reuse logic.
    // The previous function generated them. Let's copy that logic briefly or extract it.
    // For simplicity, let's just do the RTree lookup again here since it's fast.
    let points: Vec<GeomWithData<[f64; 2], usize>> = stops
        .iter()
        .enumerate()
        .map(|(i, s)| {
            GeomWithData::new(
                [
                    s.point.as_ref().map(|p| p.x).unwrap_or(0.0),
                    s.point.as_ref().map(|p| p.y).unwrap_or(0.0),
                ],
                i,
            )
        })
        .collect();
    let rtree = RTree::bulk_load(points);

    for (i, stop) in stops.iter().enumerate() {
        let lon = stop.point.as_ref().map(|p| p.x).unwrap_or(0.0);
        let lat = stop.point.as_ref().map(|p| p.y).unwrap_or(0.0);

        let nearest = rtree.locate_within_distance([lon, lat], 0.003);
        for point in nearest {
            let neighbor_idx = point.data;
            if i == neighbor_idx {
                continue;
            }

            let n_lon = point.geom()[0];
            let n_lat = point.geom()[1];
            let dist = utils::haversine_distance(lat, lon, n_lat, n_lon);

            if dist <= 300.0 {
                let walk_seconds = (dist / 1.4) as u32;
                adj[i].push((neighbor_idx, walk_seconds));
            }
        }
    }

    println!(
        "  - Static graph built. Nodes: {}, Edges: ~{}",
        stops.len(),
        adj.iter().map(|v| v.len()).sum::<usize>()
    );

    // 2. Run Dijkstra from random samples

    // 2. Run Dijkstra from samples
    let start_nodes: Vec<usize> = if sample_size >= stops.len() {
        (0..stops.len()).collect()
    } else {
        let mut rng = rand::rng();
        (0..sample_size)
            .map(|_| rng.random_range(0..stops.len()))
            .collect()
    };

    let centrality_updates: Vec<Vec<(usize, u32)>> = start_nodes
        .into_par_iter()
        .map(|start_node| {
            let mut local_centrality = Vec::new();

            // Dijkstra
            let mut dist = vec![u32::MAX; stops.len()];
            let mut pq = BinaryHeap::new();

            dist[start_node] = 0;
            pq.push(std::cmp::Reverse((0, start_node)));

            // Track predecessors to reconstruct paths?
            // Or just count nodes visited in the shortest path tree?
            // "The stations being on the largest number of shortest paths are chosen as hubs."
            // This usually means Betweenness Centrality.
            // Exact BC is expensive.
            // Approximation: For a single SSSP from s, increment centrality for all v on the shortest path s->t for all t.
            // This is equivalent to counting how many descendants each node has in the Shortest Path Tree (SPT), roughly?
            // Or just: if v is on the shortest path from s to t, increment v.
            // To do this efficiently:
            // 1. Run Dijkstra to get SPT (predecessors).
            // 2. Accumulate counts from leaves up to root.
            //    For each node u, `dependency[u] = 1 + sum(dependency[v])` for v where parent[v] == u.
            //    Then add `dependency[u]` to global centrality.

            // Let's implement predecessors.
            let mut predecessors: Vec<Vec<usize>> = vec![Vec::new(); stops.len()];
            // Note: Multiple predecessors possible for equal cost paths.

            // Limit search depth/cost?
            // "Cost-limited Dijkstra searches".
            // Let's limit to 2 hours (7200s).
            let max_cost = 7200;

            let mut stack = Vec::new(); // For traversal order

            while let Some(std::cmp::Reverse((d, u))) = pq.pop() {
                if d > dist[u] {
                    continue;
                }
                if d > max_cost {
                    break;
                }

                stack.push(u);

                for &(v, weight) in &adj[u] {
                    let new_dist = d + weight;
                    if new_dist < dist[v] {
                        dist[v] = new_dist;
                        predecessors[v].clear();
                        predecessors[v].push(u);
                        pq.push(std::cmp::Reverse((new_dist, v)));
                    } else if new_dist == dist[v] {
                        predecessors[v].push(u);
                    }
                }
            }

            // Accumulate centrality (Brandes algorithm simplified for unweighted count on DAG of SPs)
            // We want to count how many shortest paths pass through v.
            // Let sigma[v] = number of shortest paths from s to v.
            // Let delta[v] = dependency of s on v.
            // delta[v] = sum_{w: v in pred[w]} (sigma[v]/sigma[w]) * (1 + delta[w])
            // This is getting complicated for a quick heuristic.

            // Simpler heuristic from paper context:
            // "The stations being on the largest number of shortest paths"
            // Maybe just: if a node is visited by Dijkstra, it's "reachable".
            // But that makes close nodes high centrality? No.
            // Hubs are central.
            // Let's use the dependency accumulation (Brandes).
            // It's not that hard since we have the stack (topological order).

            // 1. Compute sigma (path counts) - Forward pass
            // We need to do this during Dijkstra or re-traverse.
            // Actually, we can just do it in `stack` order (increasing distance).
            // Wait, stack is extraction order, which is correct for forward pass.
            let mut sigma = vec![0.0; stops.len()];
            sigma[start_node] = 1.0;

            // We need to process in distance order. `stack` contains nodes in increasing distance order.
            // But we didn't store edges in stack.
            // We can re-evaluate edges or store them.
            // Re-evaluating is fine.
            // Actually, we have `predecessors`.
            // We can compute sigma using predecessors.
            // But predecessors point backwards.
            // So to compute sigma[v], we need sigma[u] for u in predecessors[v].
            // Since we process in increasing distance, predecessors are already processed.
            for &u in &stack {
                if u == start_node {
                    continue;
                }
                for &p in &predecessors[u] {
                    sigma[u] += sigma[p];
                }
            }

            // 2. Compute delta (dependency) - Backward pass
            let mut delta = vec![0.0; stops.len()];
            // Process stack in reverse
            for &w in stack.iter().rev() {
                for &v in &predecessors[w] {
                    if sigma[w] > 0.0 {
                        delta[v] += (sigma[v] / sigma[w]) * (1.0 + delta[w]);
                    }
                }
                if w != start_node {
                    // Add to local centrality
                    // We can just add to a list and sum later
                    // Or return a map.
                    // Since we map to `local_centrality`, let's just push (w, count).
                    // But we want to sum up `delta[w]`.
                    // Since `delta` is float, let's cast or just count "is on path".
                    // The paper says "largest number of shortest paths".
                    // This implies betweenness.
                }
            }

            // Collect results
            for (i, &d) in delta.iter().enumerate() {
                if d > 0.0 {
                    // We can just use the integer part or round.
                    // Or just add 1 if d > threshold?
                    // Let's just accumulate the raw value (scaled) or just count occurrences?
                    // "Number of shortest paths" usually means count of (s, t) pairs.
                    // delta[v] is exactly that count (fractional if multiple paths).
                    // So we should add delta[v] to global centrality.
                    // But we can't return float easily in `Vec<Vec<usize>>`.
                    // Let's return `Vec<(usize, f64)>`?
                    // Or just cast to usize * 1000 for precision.
                    if d >= 1.0 {
                        local_centrality.push(i);
                    }
                }
            }
            // Wait, if we just return `local_centrality` as list of nodes, we lose the magnitude of delta.
            // The previous code did `centrality[hub_idx] += 1`.
            // That was "is this node a hub in this query?".
            // In the previous code, `local_hubs` was "nodes where transfers happen".
            // Here we are doing global centrality.
            // Let's change the return type of the map to `Vec<(usize, f32)>`.

            // Actually, to fit the existing structure `Vec<Vec<usize>>` (list of hubs found),
            // we might want to just return nodes that have high dependency?
            // No, we should accumulate the values.
            // Let's change the accumulator loop below.

            // For now, let's just return the nodes that have non-zero dependency,
            // but that treats a node with delta=1 same as delta=1000.
            // We should probably return `Vec<(usize, u32)>` (node, score).

            // Let's hack it: return `Vec<usize>` where we repeat the node `delta` times?
            // That might be huge.
            // Let's change the `centrality_updates` type.

            // Since I cannot change the type signature of the `collect` easily without changing the variable type,
            // I will do that.

            // But wait, I am inside `map`.
            // I will return `Vec<(usize, u32)>`.
            let mut updates = Vec::with_capacity(stack.len());
            for (i, &d) in delta.iter().enumerate() {
                if i == start_node {
                    continue;
                }
                if d > 0.001 {
                    updates.push((i, (d * 10.0) as u32)); // Scale by 10 to keep some precision
                }
            }
            updates
        })
        .collect();

    let mut centrality = vec![0; stops.len()];
    for updates in centrality_updates {
        for (node, score) in updates {
            centrality[node] += score as usize;
        }
    }

    // 4. Select Top K
    let mut indexed_centrality: Vec<(usize, usize)> = centrality.into_iter().enumerate().collect();
    indexed_centrality.sort_by(|a, b| b.1.cmp(&a.1)); // Descending

    let result: HashSet<usize> = indexed_centrality
        .iter()
        .filter(|(_, c)| *c > 0)
        .take(top_k)
        .map(|(i, _)| *i)
        .collect();

    println!("  - Identified Hubs: {:?}", result);

    result
}

async fn stitch_graph(args: &Args) -> Result<()> {
    println!("Stitching graph from chunks in {:?}", args.output);

    // 1. Load Manifest
    let manifest_path = args.output.join("manifest.json");
    let file = File::open(&manifest_path).context("Failed to open manifest")?;
    let reader = BufReader::new(file);
    let manifest: Manifest = serde_json::from_reader(reader)?;

    // 2. Identify all partitions
    let mut partitions = HashSet::new();
    for p_list in manifest.chateau_to_partitions.values() {
        for &p in p_list {
            partitions.insert(p);
        }
    }
    let mut sorted_partitions: Vec<u32> = partitions.into_iter().collect();
    sorted_partitions.sort();

    println!("Found {} partitions", sorted_partitions.len());

    // 3. Load Chunks and Identify Long-Distance Stations
    let mut loaded_partitions: HashMap<u32, TransitPartition> = HashMap::new();
    let mut long_distance_stops_per_partition: HashMap<u32, HashSet<u32>> = HashMap::new();

    for &pid in &sorted_partitions {
        let path = args.output.join(format!("transit_chunk_{}.bincode", pid));
        let mut partition: TransitPartition =
            load_bincode(path.to_str().unwrap()).context("Failed to load transit partition")?;

        let mut long_dist_stop_indices = HashSet::new();

        for pattern in &partition.trip_patterns {
            let dir_idx = pattern.direction_pattern_idx as usize;
            if dir_idx >= partition.direction_patterns.len() {
                continue;
            }
            let stop_indices = &partition.direction_patterns[dir_idx].stop_indices;

            if stop_indices.len() < 2 {
                continue;
            }

            let mut total_dist = 0.0;
            for i in 0..stop_indices.len() - 1 {
                let s1 = &partition.stops[stop_indices[i] as usize];
                let s2 = &partition.stops[stop_indices[i + 1] as usize];
                total_dist += crate::utils::haversine_distance(s1.lat, s1.lon, s2.lat, s2.lon);
            }

            if total_dist > 100_000.0 {
                // 100km
                for &s_idx in stop_indices {
                    long_dist_stop_indices.insert(s_idx);
                }
            }
        }

        // Update is_long_distance flag on stops
        for (idx, stop) in partition.stops.iter_mut().enumerate() {
            if long_dist_stop_indices.contains(&(idx as u32)) {
                stop.is_long_distance = true;
            } else {
                stop.is_long_distance = false;
            }
        }

        long_distance_stops_per_partition.insert(pid, long_dist_stop_indices);
        loaded_partitions.insert(pid, partition);
    }

    // 4. Load Cross-Partition Edges and Build Global Node Map
    // We only want to include nodes in the global index if they are:
    // a) Long-distance stations (must be hubs)
    // b) Border nodes involved in cross-partition edges (to ensure connectivity)

    // First, load edges to identify border nodes that are actually used
    let mut cross_partition_edges: Vec<((usize, usize), DagEdge)> = Vec::new();
    let mut active_border_nodes: HashSet<(String, String)> = HashSet::new(); // (chateau, id)

    // Temporary storage for raw edges before we have global indices
    struct RawEdge {
        from_chateau: String,
        from_id: String,
        to_chateau: String,
        to_id: String,
        edge_type: Option<EdgeType>,
    }
    let mut raw_edges: Vec<RawEdge> = Vec::new();

    for &pid in &sorted_partitions {
        let edge_path = args.output.join(format!("edges_chunk_{}.json", pid));
        if edge_path.exists() {
            let file = File::open(&edge_path).context("Failed to open edge chunk {}")?;
            let reader = BufReader::new(file);
            let edges: Vec<EdgeEntry> = serde_json::from_reader(reader)?;

            for edge in edges {
                active_border_nodes.insert((edge.from_chateau.clone(), edge.from_id.clone()));
                active_border_nodes.insert((edge.to_chateau.clone(), edge.to_id.clone()));

                raw_edges.push(RawEdge {
                    from_chateau: edge.from_chateau,
                    from_id: edge.from_id,
                    to_chateau: edge.to_chateau,
                    to_id: edge.to_id,
                    edge_type: edge.edge_type,
                });
            }
        }
    }

    // Now build global node map
    let mut global_node_map: HashMap<(String, String), (u32, u32)> = HashMap::new();
    let mut all_global_nodes: HashMap<u32, Vec<TransitStop>> = HashMap::new();
    let mut node_to_global_idx: HashMap<(u32, u32), usize> = HashMap::new();
    let mut global_to_partition_map: HashMap<usize, (u32, u32)> = HashMap::new();
    let mut next_global_idx = 0;

    for &pid in &sorted_partitions {
        let partition = loaded_partitions.get(&pid).unwrap();
        let long_dist_indices = long_distance_stops_per_partition.get(&pid).unwrap();

        for (local_idx, stop) in partition.stops.iter().enumerate() {
            let key = (
                partition.chateau_ids[stop.chateau_idx as usize].clone(),
                stop.station_id.clone(),
            );

            let is_long_dist = stop.is_hub && long_dist_indices.contains(&(local_idx as u32));
            let is_active_border = active_border_nodes.contains(&key);

            if is_long_dist || is_active_border {
                global_node_map.insert(key, (pid, local_idx as u32));

                // Assign global index
                let g_idx = next_global_idx;
                next_global_idx += 1;
                node_to_global_idx.insert((pid, local_idx as u32), g_idx);
                global_to_partition_map.insert(g_idx, (pid, local_idx as u32));

                all_global_nodes.entry(pid).or_default().push(stop.clone());
            }
        }
    }

    println!(
        "Global Graph: {} nodes (Long-Distance + Active Border)",
        next_global_idx
    );

    // Convert raw edges to global edges
    for edge in raw_edges {
        let from_key = (edge.from_chateau, edge.from_id);
        let to_key = (edge.to_chateau, edge.to_id);

        if let (Some(&(p1, l1)), Some(&(p2, l2))) =
            (global_node_map.get(&from_key), global_node_map.get(&to_key))
        {
            if let (Some(&g1), Some(&g2)) = (
                node_to_global_idx.get(&(p1, l1)),
                node_to_global_idx.get(&(p2, l2)),
            ) {
                if let Some(et) = edge.edge_type {
                    let dag_edge = DagEdge {
                        from_node_idx: g1 as u32,
                        to_node_idx: g2 as u32,
                        edge_type: Some(et),
                    };
                    cross_partition_edges.push(((g1, g2), dag_edge));
                }
            }
        }
    }

    // 5. Compute Global Patterns
    // Note: We no longer pass intra_partition_edges as they are handled by local_transfer_patterns implicitly
    compute_global_patterns(
        &all_global_nodes,
        &cross_partition_edges,
        &global_to_partition_map,
        &mut loaded_partitions,
        &args.output,
    );

    // 6b. Compute Border Patterns (Step 4)
    let partition_dags = compute_border_patterns(
        &all_global_nodes,
        &loaded_partitions,
        &global_to_partition_map,
    );

    // Save Global Pattern Index
    let global_index = GlobalPatternIndex {
        partition_dags,
        long_distance_dags: Vec::new(), // TODO: Populate if needed, or maybe Step 3 should return these?
    };
    let global_path = args.output.join("global_patterns.bincode");
    if let Err(e) = save_bincode(&global_index, global_path.to_str().unwrap()) {
        eprintln!("Failed to save global_patterns.bincode: {}", e);
    } else {
        println!("  - Saved global_patterns.bincode");
    }

    // 6. Save Updated Partitions
    for (pid, partition) in loaded_partitions {
        let path = args.output.join(format!("transit_chunk_{}.bincode", pid));
        if let Err(e) = save_bincode(&partition, path.to_str().unwrap()) {
            eprintln!("Failed to save partition {}: {}", pid, e);
        } else {
            println!("  - Saved updated partition {} to {:?}", pid, path);
        }
    }

    Ok(())
}
