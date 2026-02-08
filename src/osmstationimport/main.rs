// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// OSM Station Import Tool - Imports railway stations from OSM PBF files

#![deny(
    clippy::mutable_key_type,
    clippy::map_entry,
    clippy::boxed_local,
    clippy::let_unit_value,
    clippy::redundant_allocation,
    clippy::bool_comparison,
    clippy::bind_instead_of_map,
    clippy::vec_box,
    clippy::while_let_loop,
    clippy::useless_asref,
    clippy::repeat_once,
    clippy::deref_addrof,
    clippy::suspicious_map,
    clippy::arc_with_non_send_sync,
    clippy::single_char_pattern,
    clippy::for_kv_map,
    clippy::let_unit_value,
    clippy::let_and_return,
    clippy::iter_nth,
    clippy::iter_cloned_collect
)]

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use catenary::models::{OsmStation, OsmStationImport};
use catenary::postgres_tools::{CatenaryPostgresPool, make_async_pool};
use clap::Parser;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use geo::{Distance, Haversine, Point};
use osmpbfreader::{OsmId, OsmObj, OsmPbfReader, Relation};
use regex::Regex;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs::File;
use std::io::{BufReader, Read, Seek};
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(version, about = "Import OSM railway stations from PBF files", long_about = None)]
struct Args {
    /// Path to OSM PBF file (pre-filtered with railway station tags)
    #[arg(long)]
    file: String,

    /// Force re-import even if file was already imported
    #[arg(long, default_value_t = false)]
    force: bool,
}

/// Compute SHA256 hash of a file
fn compute_file_hash(path: &str) -> Result<String, Box<dyn Error + Send + Sync>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 8192];

    loop {
        let bytes_read = reader.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }

    Ok(format!("{:x}", hasher.finalize()))
}

/// Classify mode type based on OSM tags
fn classify_mode(tags: &osmpbfreader::Tags) -> Option<String> {
    // Check station=* tag first (most specific indicator)
    if let Some(station_type) = tags.get("station") {
        match station_type.as_str() {
            "subway" => return Some("subway".to_string()),
            "light_rail" => return Some("light_rail".to_string()),
            "train" => return Some("rail".to_string()),
            "monorail" => return Some("monorail".to_string()),
            "funicular" => return Some("funicular".to_string()),
            "tram" => return Some("tram".to_string()),
            _ => {} // Continue to other checks
        }
    }

    // Check for subway
    if tags.get("subway").map_or(false, |v| v == "yes")
        || tags.get("railway").map_or(false, |v| v == "subway")
    {
        return Some("subway".to_string());
    }

    // Check for monorail
    if tags.get("monorail").map_or(false, |v| v == "yes")
        || tags.get("railway").map_or(false, |v| v == "monorail")
    {
        return Some("monorail".to_string());
    }

    // Check for funicular
    if tags.get("railway").map_or(false, |v| v == "funicular") {
        return Some("funicular".to_string());
    }

    // Check for tram
    if tags.get("tram").map_or(false, |v| v == "yes")
        || tags.get("railway").map_or(false, |v| v == "tram_stop")
    {
        return Some("tram".to_string());
    }

    // Check for rail (train)
    if tags.get("train").map_or(false, |v| v == "yes")
        || tags
            .get("railway")
            .map_or(false, |v| v == "station" || v == "halt")
    {
        return Some("rail".to_string());
    }

    // Check for light_rail
    if tags.get("light_rail").map_or(false, |v| v == "yes")
        || tags.get("railway").map_or(false, |v| v == "light_rail")
    {
        return Some("light_rail".to_string());
    }

    // For public_transport tags, try to classify by additional tags
    if tags.get("public_transport").is_some() {
        if tags.get("subway").map_or(false, |v| v == "yes") {
            return Some("subway".to_string());
        }
        if tags.get("monorail").map_or(false, |v| v == "yes") {
            return Some("monorail".to_string());
        }
        if tags.get("tram").map_or(false, |v| v == "yes") {
            return Some("tram".to_string());
        }
        if tags.get("train").map_or(false, |v| v == "yes") {
            return Some("rail".to_string());
        }
        if tags.get("light_rail").map_or(false, |v| v == "yes") {
            return Some("light_rail".to_string());
        }
        // Default to rail for unclassified public_transport stations
        return Some("rail".to_string());
    }

    None
}

/// Determine station type from tags
fn get_station_type(tags: &osmpbfreader::Tags) -> Option<String> {
    if tags.get("railway").map_or(false, |v| v == "station") {
        return Some("station".to_string());
    }
    if tags.get("railway").map_or(false, |v| v == "halt") {
        return Some("halt".to_string());
    }
    if tags.get("railway").map_or(false, |v| v == "tram_stop") {
        return Some("tram_stop".to_string());
    }
    if tags
        .get("public_transport")
        .map_or(false, |v| v == "stop_position")
    {
        return Some("stop_position".to_string());
    }
    if tags
        .get("public_transport")
        .map_or(false, |v| v == "platform")
    {
        return Some("platform".to_string());
    }
    if tags
        .get("public_transport")
        .map_or(false, |v| v == "station")
    {
        return Some("station".to_string());
    }
    None
}

/// Check if this OSM object should be included as a railway station/platform
fn is_railway_station_or_platform(tags: &osmpbfreader::Tags) -> bool {
    // Exclude Disney Parks
    if let Some(operator) = tags.get("operator") {
        if operator == "Disney Parks" {
            return false;
        }
    }

    // Exclude railway=proposed
    if tags.get("railway").map_or(false, |v| v == "proposed") {
        return false;
    }

    // Exclude bus-only stations
    if tags.get("highway").map_or(false, |v| v == "bus_stop")
        && tags.get("railway").is_none()
        && tags.get("train").is_none()
        && tags.get("tram").is_none()
        && tags.get("subway").is_none()
        && tags.get("light_rail").is_none()
    {
        return false;
    }

    // Include railway stations
    if let Some(railway) = tags.get("railway") {
        let rail_types = ["station", "halt", "tram_stop", "subway", "platform"];
        if rail_types.contains(&railway.as_str()) {
            return true;
        }
    }

    // Include subway stations
    if tags.get("station").map_or(false, |v| v == "subway") {
        return true;
    }

    // Include public_transport stations/platforms that are rail/tram/subway
    if let Some(pt) = tags.get("public_transport") {
        if pt == "station" || pt == "stop_position" || pt == "platform" {
            // Only include if it has rail/tram/subway context
            if tags.get("train").is_some()
                || tags.get("tram").is_some()
                || tags.get("subway").is_some()
                || tags.get("light_rail").is_some()
                || tags.get("railway").is_some()
            {
                return true;
            }
        }
    }

    false
}

/// Check if a relation is a stop_area relation (groups platforms with their parent station)
fn is_stop_area_relation(tags: &osmpbfreader::Tags) -> bool {
    tags.get("public_transport")
        .map_or(false, |v| v == "stop_area")
        || tags
            .get("type")
            .map_or(false, |v| v == "public_transport" || v == "site")
}

/// Find the parent station node ID from a stop_area relation
/// Returns (parent_osm_id, parent_name) where parent is the main station
fn find_parent_station_in_relation(
    rel: &Relation,
    node_set: &HashSet<i64>,
) -> Option<(i64, Option<String>)> {
    // First, look for members with role "label" or empty role that are stations
    // The "label" role typically indicates the main station name node

    let mut label_node: Option<i64> = None;
    let mut station_node: Option<i64> = None;

    for member in &rel.refs {
        if let OsmId::Node(node_id) = member.member {
            let id = node_id.0;
            // Only consider nodes that are in our station set
            if !node_set.contains(&id) {
                continue;
            }

            let role = member.role.as_str();

            // "label" role is the primary name node
            if role == "label" {
                label_node = Some(id);
            }
            // Empty role or "stop" typically indicates the main station
            else if role.is_empty() || role == "stop" || role == "station" {
                if station_node.is_none() {
                    station_node = Some(id);
                }
            }
        }
    }

    // Prefer label node, then station node
    if let Some(id) = label_node {
        return Some((id, None));
    }
    if let Some(id) = station_node {
        return Some((id, None));
    }

    None
}

/// Get all platform/child node IDs from a stop_area relation
fn get_platform_nodes_in_relation(rel: &Relation) -> Vec<i64> {
    let mut platforms = Vec::new();

    for member in &rel.refs {
        if let OsmId::Node(node_id) = member.member {
            let role = member.role.as_str();
            // Platforms have role "platform" or nodes with local_ref
            if role == "platform" || role == "stop" || role.is_empty() {
                platforms.push(node_id.0);
            }
        }
    }

    platforms
}

/// Apply name overrides
fn get_clean_name(name: &str) -> String {
    match name {
        "Santa Ana Regional Transportation Center" => "Santa Ana".to_string(),
        _ => name.to_string(),
    }
}

/// Extract multilingual names from tags
fn extract_name_translations(tags: &osmpbfreader::Tags) -> Option<serde_json::Value> {
    let mut translations: HashMap<String, String> = HashMap::new();

    for (key, value) in tags.iter() {
        if key.starts_with("name:") {
            let lang = key.strip_prefix("name:").unwrap();
            translations.insert(lang.to_string(), value.to_string());
        }
    }

    // Add official_name, alt_name, short_name if present
    if let Some(official_name) = tags.get("official_name") {
        translations.insert("official".to_string(), official_name.to_string());
    }
    if let Some(alt_name) = tags.get("alt_name") {
        translations.insert("alt".to_string(), alt_name.to_string());
    }
    if let Some(short_name) = tags.get("short_name") {
        translations.insert("short".to_string(), short_name.to_string());
    }

    if translations.is_empty() {
        None
    } else {
        Some(serde_json::to_value(translations).unwrap())
    }
}

/// Intermediate struct for storing node data during first pass
#[derive(Clone)]
struct NodeData {
    id: i64,
    lat: f64,
    lon: f64,
    name: Option<String>,
    name_translations: Option<serde_json::Value>,
    station_type: Option<String>,
    railway_tag: Option<String>,
    mode_type: String,
    uic_ref: Option<String>,
    ref_: Option<String>,
    wikidata: Option<String>,
    operator: Option<String>,
    network: Option<String>,
    level: Option<String>,
    local_ref: Option<String>,
    is_derivative: bool,
}

/// Intermediate struct for storing way data that needs centroid calculation
#[derive(Clone)]
struct WayData {
    id: i64,
    node_refs: Vec<i64>,
    name: Option<String>,
    name_translations: Option<serde_json::Value>,
    station_type: Option<String>,
    railway_tag: Option<String>,
    mode_type: String,
    uic_ref: Option<String>,
    ref_: Option<String>,
    wikidata: Option<String>,
    operator: Option<String>,
    network: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args = Args::parse();

    println!("OSM Station Import Tool");
    println!("Processing file: {}", args.file);

    // Compute file hash
    println!("Computing file hash...");
    let file_hash = compute_file_hash(&args.file)?;
    println!("File hash: {}", file_hash);

    // Extract filename
    let file_name = std::path::Path::new(&args.file)
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| args.file.clone());

    // Connect to database
    println!("Connecting to database...");
    let conn_pool: CatenaryPostgresPool = make_async_pool().await?;
    let arc_conn_pool: Arc<CatenaryPostgresPool> = Arc::new(conn_pool);
    let conn = &mut arc_conn_pool.get().await?;

    // Check if this file has already been imported
    use catenary::schema::gtfs::osm_station_imports::dsl as imports_dsl;

    let existing_import: Option<OsmStationImport> = imports_dsl::osm_station_imports
        .filter(imports_dsl::file_name.eq(&file_name))
        .filter(imports_dsl::file_hash.eq(&file_hash))
        .first(conn)
        .await
        .optional()?;

    if let Some(existing) = existing_import {
        if args.force {
            println!(
                "File already imported (import_id: {}), but --force specified. Deleting existing data...",
                existing.import_id
            );

            // Delete existing stations
            use catenary::schema::gtfs::osm_stations::dsl as stations_dsl;
            let deleted_count = diesel::delete(
                stations_dsl::osm_stations.filter(stations_dsl::import_id.eq(existing.import_id)),
            )
            .execute(conn)
            .await?;
            println!("Deleted {} existing stations", deleted_count);

            // Delete the import record
            diesel::delete(
                imports_dsl::osm_station_imports
                    .filter(imports_dsl::import_id.eq(existing.import_id)),
            )
            .execute(conn)
            .await?;
            println!("Deleted import record");
        } else {
            println!(
                "File already imported with {} stations (import_id: {})",
                existing.station_count, existing.import_id
            );
            println!("Skipping import. Use --force to re-import.");
            return Ok(());
        }
    }

    use catenary::schema::gtfs::osm_stations::dsl as stations_dsl;

    // Create import record
    println!("Creating import record...");
    let new_import = diesel::insert_into(imports_dsl::osm_station_imports)
        .values((
            imports_dsl::file_name.eq(&file_name),
            imports_dsl::file_hash.eq(&file_hash),
            imports_dsl::station_count.eq(0),
        ))
        .returning(imports_dsl::import_id)
        .get_result::<i32>(conn)
        .await?;

    let import_id = new_import;
    println!("Import ID: {}", import_id);

    // =========================================================================
    // PASS 1: Collect all station/platform nodes and stop_area relations
    // =========================================================================
    println!("\n=== Pass 1: Collecting nodes and relations ===");

    let file = File::open(&args.file)?;
    let mut reader = OsmPbfReader::new(file);

    // Maps to store data from first pass
    let mut node_data: HashMap<i64, NodeData> = HashMap::new();
    let mut node_ids: HashSet<i64> = HashSet::new();
    let mut stop_area_relations: Vec<Relation> = Vec::new();
    let mut way_data: HashMap<i64, WayData> = HashMap::new();
    let mut railway_station_relations: Vec<Relation> = Vec::new();
    let mut all_node_coords: HashMap<i64, (f64, f64)> = HashMap::new();

    let mut processed = 0u64;
    let mut nodes_found = 0u64;
    let mut relations_found = 0u64;
    let mut ways_found = 0u64;

    for obj_result in reader.iter() {
        let obj = obj_result?;
        processed += 1;

        if processed % 100000 == 0 {
            print!(
                "\rPass 1: Processed {} objects, found {} nodes, {} ways, {} relations...",
                processed, nodes_found, ways_found, relations_found
            );
            std::io::Write::flush(&mut std::io::stdout())?;
        }

        match obj {
            OsmObj::Node(node) => {
                let id = node.id.0;
                all_node_coords.insert(id, (node.lat(), node.lon()));

                if !is_railway_station_or_platform(&node.tags) {
                    continue;
                }

                let mode = match classify_mode(&node.tags) {
                    Some(m) => m,
                    None => continue,
                };

                nodes_found += 1;
                node_ids.insert(id);

                node_data.insert(
                    id,
                    NodeData {
                        id,
                        lat: node.lat(),
                        lon: node.lon(),
                        name: node.tags.get("name").map(|s| get_clean_name(s)),
                        name_translations: extract_name_translations(&node.tags),
                        station_type: get_station_type(&node.tags),
                        railway_tag: node.tags.get("railway").map(|s| s.to_string()),
                        mode_type: mode,
                        uic_ref: node.tags.get("uic_ref").map(|s| s.to_string()),
                        ref_: node.tags.get("ref").map(|s| s.to_string()),
                        wikidata: node.tags.get("wikidata").map(|s| s.to_string()),
                        operator: node.tags.get("operator").map(|s| s.to_string()),
                        network: node.tags.get("network").map(|s| s.to_string()),
                        level: node.tags.get("level").map(|s| s.to_string()),
                        local_ref: node.tags.get("local_ref").map(|s| s.to_string()),
                        is_derivative: false,
                    },
                );
            }
            OsmObj::Relation(rel) => {
                if is_stop_area_relation(&rel.tags) {
                    relations_found += 1;
                    stop_area_relations.push(rel.clone());
                }
                if is_railway_station_or_platform(&rel.tags) {
                    if let Some(mode) = classify_mode(&rel.tags) {
                        if mode == "rail" {
                            railway_station_relations.push(rel);
                        }
                    }
                }
            }
            OsmObj::Way(way) => {
                if !is_railway_station_or_platform(&way.tags) {
                    continue;
                }

                let mode = match classify_mode(&way.tags) {
                    Some(m) => m,
                    None => continue,
                };

                // if mode != "rail" {
                //    continue;
                // }

                ways_found += 1;
                let node_refs: Vec<i64> = way.nodes.iter().map(|n| n.0).collect();

                way_data.insert(
                    way.id.0,
                    WayData {
                        id: way.id.0,
                        node_refs,
                        name: way.tags.get("name").map(|s| get_clean_name(s)),
                        name_translations: extract_name_translations(&way.tags),
                        station_type: get_station_type(&way.tags),
                        railway_tag: way.tags.get("railway").map(|s| s.to_string()),
                        mode_type: mode,
                        uic_ref: way.tags.get("uic_ref").map(|s| s.to_string()),
                        ref_: way.tags.get("ref").map(|s| s.to_string()),
                        wikidata: way.tags.get("wikidata").map(|s| s.to_string()),
                        operator: way.tags.get("operator").map(|s| s.to_string()),
                        network: way.tags.get("network").map(|s| s.to_string()),
                    },
                );
            }
        }
    }

    println!(
        "\nPass 1 complete: {} nodes, {} ways, {} stop_area relations",
        nodes_found, ways_found, relations_found
    );

    // =========================================================================
    // PASS 2: Build parent mapping from stop_area relations
    // =========================================================================
    println!("\n=== Pass 2: Building parent mappings from relations ===");

    // Map from node_id -> parent_osm_id
    let mut parent_map: HashMap<i64, i64> = HashMap::new();
    let mut ways_covered_by_relations: HashSet<i64> = HashSet::new();
    let mut relations_processed = 0;
    let mut mappings_created = 0;

    for rel in &stop_area_relations {
        relations_processed += 1;

        // Check if this relation contains any of our candidate ways
        // Check if this relation contains any of our candidate ways
        let mut relation_ways: Vec<(i64, String)> = Vec::new();
        let mut primary_node_modes: HashSet<String> = HashSet::new();

        for member in &rel.refs {
            match member.member {
                OsmId::Way(way_id) => {
                    if let Some(way) = way_data.get(&way_id.0) {
                        relation_ways.push((way_id.0, way.mode_type.clone()));
                    }
                }
                OsmId::Node(node_id) => {
                    if let Some(node) = node_data.get(&node_id.0) {
                        // Check if this is a primary station node (rail/subway/tram station)
                        if node.mode_type == "rail"
                            || node.mode_type == "subway"
                            || node.mode_type == "tram"
                        {
                            // stricter check: must be a station/halt/stop, not just a platform
                            if node.station_type.as_deref() == Some("station") 
                                || node.station_type.as_deref() == Some("halt")
                                || node.station_type.as_deref() == Some("tram_stop") 
                                || node.station_type.as_deref() == Some("subway_entrance") // sometimes entrances are main nodes
                                || node.railway_tag.as_deref() == Some("station")
                                || node.railway_tag.as_deref() == Some("halt")
                            {
                                primary_node_modes.insert(node.mode_type.clone());
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        // If relation has a primary node OF THE SAME MODE, mark the way as "covered"
        // so we don't create derivative points for them
        for (way_id, way_mode) in relation_ways {
            if primary_node_modes.contains(&way_mode) {
                // EXCEPTION: Always ensure Lucien-L'Allier 555589144 gets a derivative point
                // This way has railway=station train=yes but tends to get suppressed by other logic
                if way_id == 555589144 {
                    continue;
                }
                ways_covered_by_relations.insert(way_id);
            }
        }

        // Find the parent station in this relation
        if let Some((parent_id, _)) = find_parent_station_in_relation(rel, &node_ids) {
            // Get all platform/child nodes in this relation
            let platform_nodes = get_platform_nodes_in_relation(rel);

            for platform_id in platform_nodes {
                // Don't map a node to itself
                if platform_id != parent_id && node_ids.contains(&platform_id) {
                    // Check if this node has local_ref (indicating it's a platform)
                    if let Some(node) = node_data.get(&platform_id) {
                        if node.local_ref.is_some()
                            || node.station_type.as_deref() == Some("platform")
                        {
                            parent_map.insert(platform_id, parent_id);
                            mappings_created += 1;
                        }
                    }
                }
            }
        }
    }

    println!(
        "Pass 2 complete: {} relations processed, {} parent mappings created",
        relations_processed, mappings_created
    );

    // =========================================================================
    // PASS 3: Create OsmStation records with parent_osm_id populated
    // =========================================================================
    println!("\n=== Pass 3: Creating station records ===");

    let mut stations: Vec<OsmStation> = Vec::new();

    for (id, data) in &node_data {
        let parent_osm_id = parent_map.get(id).copied();

        let station = OsmStation {
            osm_id: *id,
            osm_type: "node".to_string(),
            import_id,
            point: postgis_diesel::types::Point {
                x: data.lon,
                y: data.lat,
                srid: Some(4326),
            },
            name: data.name.clone(),
            name_translations: data.name_translations.clone(),
            station_type: data.station_type.clone(),
            railway_tag: data.railway_tag.clone(),
            mode_type: data.mode_type.clone(),
            uic_ref: data.uic_ref.clone(),
            ref_: data.ref_.clone(),
            wikidata: data.wikidata.clone(),
            operator: data.operator.clone(),
            network: data.network.clone(),
            level: data.level.clone(),
            local_ref: data.local_ref.clone(),
            parent_osm_id,
            is_derivative: data.is_derivative,
        };

        stations.push(station);
    }

    println!(
        "Created {} station records ({} with parent mappings)",
        stations.len(),
        stations
            .iter()
            .filter(|s| s.parent_osm_id.is_some())
            .count()
    );

    // =========================================================================
    // PASS 3.5: Create derivative points from ways/relations that have no nearby rail node
    // =========================================================================
    println!("\n=== Pass 3.5: Creating derivative points from ways ===");

    fn normalize_name_for_comparison(name: &str) -> String {
        name.to_lowercase()
            .replace("-", " ")
            .replace("'", " ")
            .replace("'", " ")
            .replace("gare ", "")
            .replace("station ", "")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn names_are_similar(name1: &str, name2: &str) -> bool {
        let n1 = normalize_name_for_comparison(name1);
        let n2 = normalize_name_for_comparison(name2);

        if n1 == n2 {
            return true;
        }
        if n1.contains(&n2) || n2.contains(&n1) {
            return true;
        }
        false
    }

    let existing_stations: Vec<&NodeData> = node_data.values().collect();

    let mut derivative_count = 0;

    for (way_id, way) in &way_data {
        // Skip purely visual building/platform ways that are already part of a station relation containing a main node
        if ways_covered_by_relations.contains(way_id) {
            continue;
        }

        let coords: Vec<(f64, f64)> = way
            .node_refs
            .iter()
            .filter_map(|nid| all_node_coords.get(nid).copied())
            .collect();

        if coords.is_empty() {
            continue;
        }

        let centroid_lat = coords.iter().map(|(lat, _)| lat).sum::<f64>() / coords.len() as f64;
        let centroid_lon = coords.iter().map(|(_, lon)| lon).sum::<f64>() / coords.len() as f64;

        let has_nearby_rail_node = existing_stations.iter().any(|node| {
            let centroid_point = Point::new(centroid_lon, centroid_lat);
            let node_point = Point::new(node.lon, node.lat);
            let dist = Haversine.distance(centroid_point, node_point);
            if dist > 500.0 {
                return false;
            }
            if let (Some(way_name), Some(node_name)) = (&way.name, &node.name) {
                names_are_similar(way_name, node_name)
            } else {
                true
            }
        });

        if !has_nearby_rail_node {
            derivative_count += 1;
            let station = OsmStation {
                osm_id: *way_id,
                osm_type: "way".to_string(),
                import_id,
                point: postgis_diesel::types::Point {
                    x: centroid_lon,
                    y: centroid_lat,
                    srid: Some(4326),
                },
                name: way.name.clone(),
                name_translations: way.name_translations.clone(),
                station_type: way.station_type.clone(),
                railway_tag: way.railway_tag.clone(),
                mode_type: way.mode_type.clone(),
                uic_ref: way.uic_ref.clone(),
                ref_: way.ref_.clone(),
                wikidata: way.wikidata.clone(),
                operator: way.operator.clone(),
                network: way.network.clone(),
                level: None,
                local_ref: None,
                parent_osm_id: None,
                is_derivative: true,
            };

            if let Some(name) = &way.name {
                println!(
                    "  Created derivative point for way {} ({}) at ({}, {})",
                    way_id, name, centroid_lat, centroid_lon
                );
            }

            stations.push(station);
        }
    }

    println!("Created {} derivative points from ways", derivative_count);

    // =========================================================================
    // PASS 4: Deduplicate stations
    // =========================================================================
    println!("\n=== Pass 4: Deduplicating stations ===");

    // Build a map of existing names to coordinates for lookup
    let mut existing_stations_by_name: HashMap<String, Vec<(f64, f64)>> = HashMap::new();
    for s in &stations {
        if let Some(name) = &s.name {
            existing_stations_by_name
                .entry(name.clone())
                .or_default()
                .push((s.point.y, s.point.x));
        }
    }

    let mut to_remove = HashSet::new();
    // Rule 1: Remove "....Hbf Gleis X-X...." if another station with the same name before Hbf or Hauptbahnhof exists
    let re_hbf = Regex::new(r"^(.*?)\s+(?:Hbf|Hauptbahnhof)\s+Gleis.*").unwrap();
    // Rule 2: Remove "X (tief)" if "X" exists
    let re_tief = Regex::new(r"^(.*?)\s+\(tief\)$").unwrap();

    let max_dist_meters = 1000.0;

    for (i, station) in stations.iter().enumerate() {
        if let Some(name) = &station.name {
            // Exception: If the station has a local_ref or ref, do not deduplicate it
            if station.local_ref.is_some() || station.ref_.is_some() {
                continue;
            }

            let lat = station.point.y;
            let lon = station.point.x;

            // Helper check closure
            let check_duplicates = |target_name: &str, rule_desc: &str| -> bool {
                if let Some(coords_list) = existing_stations_by_name.get(target_name) {
                    for (other_lat, other_lon) in coords_list {
                        let p1 = Point::new(lon, lat);
                        let p2 = Point::new(*other_lon, *other_lat);
                        let dist = Haversine.distance(p1, p2);
                        if dist < max_dist_meters {
                            println!(
                                "Marking duplicate station for removal: {} (found base station: {} at {:.0}m distance) [{}]",
                                name, target_name, dist, rule_desc
                            );
                            return true;
                        }
                    }
                }
                false
            };

            // Rule 1: Hbf Gleis
            if let Some(caps) = re_hbf.captures(name) {
                let base_name = caps.get(1).unwrap().as_str().trim();
                let hbf_name = format!("{} Hbf", base_name);
                let hauptbahnhof_name = format!("{} Hauptbahnhof", base_name);

                if check_duplicates(&hbf_name, "Rule 1")
                    || check_duplicates(&hauptbahnhof_name, "Rule 1")
                {
                    to_remove.insert(i);
                    continue;
                }
            }

            // Rule 2: (tief)
            if let Some(caps) = re_tief.captures(name) {
                let base_name = caps.get(1).unwrap().as_str().trim();
                if check_duplicates(base_name, "Rule 2") {
                    to_remove.insert(i);
                    continue;
                }
            }
        }
    }

    if !to_remove.is_empty() {
        println!("Removing {} duplicate stations...", to_remove.len());
        let old_len = stations.len();
        let mut new_stations = Vec::with_capacity(old_len - to_remove.len());
        for (i, station) in stations.into_iter().enumerate() {
            if !to_remove.contains(&i) {
                new_stations.push(station);
            }
        }
        stations = new_stations;
    } else {
        println!("No duplicate stations found.");
    }

    // Insert stations in batches

    println!("\nInserting {} stations into database...", stations.len());

    let batch_size = 1000;
    for (i, chunk) in stations.chunks(batch_size).enumerate() {
        diesel::insert_into(stations_dsl::osm_stations)
            .values(chunk)
            .execute(conn)
            .await?;

        if (i + 1) * batch_size % 5000 == 0 || (i + 1) * batch_size >= stations.len() {
            println!(
                "  Inserted {}/{} stations",
                std::cmp::min((i + 1) * batch_size, stations.len()),
                stations.len()
            );
        }
    }

    // Update station count
    diesel::update(imports_dsl::osm_station_imports.filter(imports_dsl::import_id.eq(import_id)))
        .set(imports_dsl::station_count.eq(stations.len() as i32))
        .execute(conn)
        .await?;

    // Delete old entries for THIS file now that new data is available
    // First, find old import IDs for this file
    println!("\nDeleting old entries for file '{}'...", file_name);
    let old_import_ids: Vec<i32> = imports_dsl::osm_station_imports
        .filter(imports_dsl::file_name.eq(&file_name))
        .filter(imports_dsl::import_id.ne(import_id))
        .select(imports_dsl::import_id)
        .load(conn)
        .await?;

    if !old_import_ids.is_empty() {
        // Delete stations with those old import IDs
        let deleted_stations = diesel::delete(
            stations_dsl::osm_stations.filter(stations_dsl::import_id.eq_any(&old_import_ids)),
        )
        .execute(conn)
        .await?;
        println!("Deleted {} old stations", deleted_stations);

        // Delete the old import records
        let deleted_imports = diesel::delete(
            imports_dsl::osm_station_imports.filter(imports_dsl::import_id.eq_any(&old_import_ids)),
        )
        .execute(conn)
        .await?;
        println!("Deleted {} old import records", deleted_imports);
    } else {
        println!("No old entries to delete");
    }

    println!("\nImport complete! {} stations imported.", stations.len());
    println!(
        "  - {} platforms with parent station mappings",
        stations
            .iter()
            .filter(|s| s.parent_osm_id.is_some())
            .count()
    );
    println!(
        "  - {} stations without parent (likely top-level stations)",
        stations
            .iter()
            .filter(|s| s.parent_osm_id.is_none())
            .count()
    );

    Ok(())
}
