use anyhow::{Context, Result};
use catenary::models::{CompressedTrip as DbCompressedTrip, ItineraryPatternRow, StopMapping};
use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::routing_common::transit_graph::{load_bincode, save_bincode};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

/// Run Local Transfer Pattern generation for a specific cluster.
/// This function fetches trips from the DB that are fully contained within the cluster.
pub async fn run_local_patterns(
    pool: Arc<CatenaryPostgresPool>,
    output_dir: &Path,
    cluster_id: u32,
) -> Result<()> {
    println!("Generating Local Patterns for Cluster {}...", cluster_id);

    let mut conn = pool.get().await.context("Failed to get DB connection")?;

    // 1. Load Station -> Cluster Map
    let map_path = output_dir.join("station_to_cluster_map.bincode");
    let station_to_cluster: HashMap<String, u32> = load_bincode(map_path.to_str().unwrap())?;

    // 2. Identify Stations in this Cluster
    let cluster_stations: HashSet<String> = station_to_cluster
        .iter()
        .filter(|&(_, &c)| c == cluster_id)
        .map(|(s, _)| s.clone())
        .collect();

    if cluster_stations.is_empty() {
        println!("Cluster {} has no stations. Skipping.", cluster_id);
        return Ok(());
    }

    println!(
        "Cluster {} has {} stations.",
        cluster_id,
        cluster_stations.len()
    );

    // 3. Fetch Stop Mappings to get GTFS Stop IDs and Chateaux
    use catenary::schema::gtfs::stop_mappings::dsl::{station_id, stop_mappings};

    // We can't query `station_id.eq_any(&cluster_stations)` if the list is huge.
    // But for a cluster (max 1500-6000 stations), it should be fine.
    // Postgres limit for IN clause is high, but let's chunk it just in case.

    let cluster_stations_vec: Vec<String> = cluster_stations.iter().cloned().collect();
    let mut all_mappings: Vec<StopMapping> = Vec::new();

    for chunk in cluster_stations_vec.chunks(1000) {
        let mappings_chunk: Vec<StopMapping> = stop_mappings
            .filter(station_id.eq_any(chunk))
            .load(&mut conn)
            .await?;
        all_mappings.extend(mappings_chunk);
    }

    // Map: Chateau -> Set<GTFS Stop ID>
    let mut stops_by_chateau: HashMap<String, HashSet<String>> = HashMap::new();
    for m in &all_mappings {
        stops_by_chateau
            .entry(m.feed_id.clone())
            .or_default()
            .insert(m.stop_id.clone());
    }

    // 4. Fetch Patterns and Trips per Chateau
    let mut valid_trips: Vec<DbCompressedTrip> = Vec::new();
    let mut valid_patterns: Vec<ItineraryPatternRow> = Vec::new();

    for (chateau_id, valid_stop_ids) in stops_by_chateau {
        println!("  - Scanning chateau {}...", chateau_id);

        // Fetch all patterns for this chateau
        // We could filter by `stop_id IN valid_stop_ids` but that only ensures *some* stops are in cluster.
        // We need *all* stops to be in cluster.
        // So we fetch all patterns for the chateau (or maybe filter by at least one stop to reduce data).
        // Filtering by "at least one stop" is better than fetching everything.

        use catenary::schema::gtfs::itinerary_pattern::dsl::{
            chateau as pattern_chateau, itinerary_pattern, stop_id,
        };

        // Get pattern IDs that touch our stops
        // We need a subquery or distinct select.
        // `SELECT DISTINCT itinerary_pattern_id FROM itinerary_pattern WHERE chateau = ? AND stop_id IN ?`

        // Diesel doesn't support `distinct` on a column easily in `select` without defining a custom struct or using `distinct_on`.
        // Let's just fetch rows where stop_id is in our set.
        // This gives us the "candidate" patterns.

        let valid_stop_ids_vec: Vec<String> = valid_stop_ids.iter().cloned().collect();
        let mut candidate_rows: Vec<ItineraryPatternRow> = Vec::new();

        for chunk in valid_stop_ids_vec.chunks(1000) {
            let rows: Vec<ItineraryPatternRow> = itinerary_pattern
                .filter(pattern_chateau.eq(&chateau_id))
                .filter(stop_id.eq_any(chunk))
                .select(ItineraryPatternRow::as_select())
                .load(&mut conn)
                .await?;
            candidate_rows.extend(rows);
        }

        // Group by pattern_id
        let mut pattern_stops: HashMap<String, HashSet<String>> = HashMap::new();
        for row in &candidate_rows {
            pattern_stops
                .entry(row.itinerary_pattern_id.clone())
                .or_default()
                .insert(row.stop_id.to_string());
        }

        // Now we need to verify if these patterns are *fully* contained.
        // The `candidate_rows` only contain rows for stops IN the cluster.
        // If a pattern has stops OUTSIDE the cluster, those rows were NOT fetched.
        // So we don't know the *full* length of the pattern from `candidate_rows` alone.
        //
        // We need to know the total number of stops in the pattern.
        // Or we need to fetch the *full* pattern for any candidate ID.

        let candidate_pattern_ids: Vec<String> = pattern_stops.keys().cloned().collect();

        if candidate_pattern_ids.is_empty() {
            continue;
        }

        // Fetch FULL patterns for candidates
        let mut full_pattern_rows: Vec<ItineraryPatternRow> = Vec::new();
        for chunk in candidate_pattern_ids.chunks(1000) {
            use catenary::schema::gtfs::itinerary_pattern::dsl::itinerary_pattern_id;
            let rows: Vec<ItineraryPatternRow> = itinerary_pattern
                .filter(pattern_chateau.eq(&chateau_id))
                .filter(itinerary_pattern_id.eq_any(chunk))
                .select(ItineraryPatternRow::as_select())
                .load(&mut conn)
                .await?;
            full_pattern_rows.extend(rows);
        }

        // Check containment
        let mut full_pattern_map: HashMap<String, Vec<ItineraryPatternRow>> = HashMap::new();
        for row in full_pattern_rows {
            full_pattern_map
                .entry(row.itinerary_pattern_id.clone())
                .or_default()
                .push(row);
        }

        let mut kept_pattern_ids: HashSet<String> = HashSet::new();

        for (pid, rows) in full_pattern_map {
            let all_in = rows
                .iter()
                .all(|r| valid_stop_ids.contains(&r.stop_id.to_string()));
            if all_in {
                kept_pattern_ids.insert(pid.clone());
                valid_patterns.extend(rows);
            }
        }

        if kept_pattern_ids.is_empty() {
            continue;
        }

        // Fetch Trips for kept patterns
        use catenary::schema::gtfs::trips_compressed::dsl::{
            chateau as trip_chateau, itinerary_pattern_id as trip_pattern_id, trips_compressed,
        };

        let kept_pattern_ids_vec: Vec<String> = kept_pattern_ids.into_iter().collect();
        for chunk in kept_pattern_ids_vec.chunks(1000) {
            let trips: Vec<DbCompressedTrip> = trips_compressed
                .filter(trip_chateau.eq(&chateau_id))
                .filter(trip_pattern_id.eq_any(chunk))
                .select(DbCompressedTrip::as_select())
                .load(&mut conn)
                .await?;
            valid_trips.extend(trips);
        }
    }

    println!(
        "  - Found {} valid trips and {} pattern rows.",
        valid_trips.len(),
        valid_patterns.len()
    );

    // TODO: Build LocalTransferPatterns from these trips
    // This requires logic similar to `compute_local_patterns_for_partition` but starting from raw trips.
    // For now, we have successfully fetched the data as requested.

    Ok(())
}
