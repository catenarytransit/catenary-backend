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
use osmpbfreader::{OsmObj, OsmPbfReader};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::{BufReader, Read};
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(version, about = "Import OSM railway stations from PBF files", long_about = None)]
struct Args {
    /// Path to OSM PBF file (pre-filtered with railway station tags)
    #[arg(long)]
    file: String,
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
    // Check for subway
    if tags.get("subway").map_or(false, |v| v == "yes")
        || tags.get("railway").map_or(false, |v| v == "subway")
        || tags.get("station").map_or(false, |v| v == "subway")
    {
        return Some("subway".to_string());
    }
    
    // Check for tram
    if tags.get("tram").map_or(false, |v| v == "yes")
        || tags.get("railway").map_or(false, |v| v == "tram_stop")
    {
        return Some("tram".to_string());
    }
    
    // Check for rail (train)
    if tags.get("train").map_or(false, |v| v == "yes")
        || tags.get("railway").map_or(false, |v| v == "station" || v == "halt")
    {
        return Some("rail".to_string());
    }
    
    // For public_transport tags, try to classify by additional tags
    if tags.get("public_transport").is_some() {
        if tags.get("subway").map_or(false, |v| v == "yes") {
            return Some("subway".to_string());
        }
        if tags.get("tram").map_or(false, |v| v == "yes") {
            return Some("tram".to_string());
        }
        if tags.get("train").map_or(false, |v| v == "yes") {
            return Some("rail".to_string());
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
    if tags.get("public_transport").map_or(false, |v| v == "stop_position") {
        return Some("stop_position".to_string());
    }
    if tags.get("public_transport").map_or(false, |v| v == "platform") {
        return Some("platform".to_string());
    }
    if tags.get("public_transport").map_or(false, |v| v == "station") {
        return Some("station".to_string());
    }
    None
}

/// Check if this OSM object should be included as a railway station
fn is_railway_station(tags: &osmpbfreader::Tags) -> bool {
    // Exclude bus-only stations
    if tags.get("highway").map_or(false, |v| v == "bus_stop") 
        && tags.get("railway").is_none() 
        && tags.get("train").is_none()
        && tags.get("tram").is_none()
        && tags.get("subway").is_none()
    {
        return false;
    }
    
    // Include railway stations
    if let Some(railway) = tags.get("railway") {
        let rail_types = ["station", "halt", "tram_stop", "subway"];
        if rail_types.contains(&railway.as_str()) {
            return true;
        }
    }
    
    // Include subway stations
    if tags.get("station").map_or(false, |v| v == "subway") {
        return true;
    }
    
    // Include public_transport stations that are rail/tram/subway
    if let Some(pt) = tags.get("public_transport") {
        if pt == "station" || pt == "stop_position" {
            // Only include if it has rail/tram/subway context
            if tags.get("train").is_some()
                || tags.get("tram").is_some()
                || tags.get("subway").is_some()
                || tags.get("railway").is_some()
            {
                return true;
            }
        }
    }
    
    false
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
        println!("File already imported with {} stations (import_id: {})", 
                 existing.station_count, existing.import_id);
        println!("Skipping import.");
        return Ok(());
    }
    
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
    
    // Parse PBF file
    println!("Parsing PBF file...");
    let file = File::open(&args.file)?;
    let mut reader = OsmPbfReader::new(file);
    
    let mut stations: Vec<OsmStation> = Vec::new();
    let mut processed = 0;
    let mut included = 0;
    
    for obj_result in reader.iter() {
        let obj = obj_result?;
        processed += 1;
        
        if processed % 10000 == 0 {
            print!("\rProcessed {} objects, found {} stations...", processed, included);
            std::io::Write::flush(&mut std::io::stdout())?;
        }
        
        match obj {
            OsmObj::Node(node) => {
                if !is_railway_station(&node.tags) {
                    continue;
                }
                
                let mode = match classify_mode(&node.tags) {
                    Some(m) => m,
                    None => continue,
                };
                
                included += 1;
                
                let station = OsmStation {
                    osm_id: node.id.0,
                    osm_type: "node".to_string(),
                    import_id,
                    point: postgis_diesel::types::Point {
                        x: node.lon(),
                        y: node.lat(),
                        srid: Some(4326),
                    },
                    name: node.tags.get("name").map(|s| s.to_string()),
                    name_translations: extract_name_translations(&node.tags),
                    station_type: get_station_type(&node.tags),
                    railway_tag: node.tags.get("railway").map(|s| s.to_string()),
                    mode_type: mode,
                    uic_ref: node.tags.get("uic_ref").map(|s| s.to_string()),
                    ref_: node.tags.get("ref").map(|s| s.to_string()),
                    wikidata: node.tags.get("wikidata").map(|s| s.to_string()),
                    operator: node.tags.get("operator").map(|s| s.to_string()),
                    network: node.tags.get("network").map(|s| s.to_string()),
                };
                
                stations.push(station);
            }
            OsmObj::Way(way) => {
                if !is_railway_station(&way.tags) {
                    continue;
                }
                
                let mode = match classify_mode(&way.tags) {
                    Some(m) => m,
                    None => continue,
                };
                
                // For ways, we need to compute centroid from nodes
                // For now, skip ways without coordinates (would need node lookup)
                // This is a simplified implementation
                included += 1;
                
                // Note: In a full implementation, you would look up the nodes
                // to compute the centroid. For now, we skip ways.
                // This can be enhanced later.
                let _ = (way, mode);
            }
            OsmObj::Relation(rel) => {
                if !is_railway_station(&rel.tags) {
                    continue;
                }
                
                // Skip relations for now - would need member lookup
                let _ = rel;
            }
        }
    }
    
    println!("\nProcessed {} objects, found {} stations", processed, included);
    
    // Insert stations in batches
    use catenary::schema::gtfs::osm_stations::dsl as stations_dsl;
    
    println!("Inserting {} stations into database...", stations.len());
    
    let batch_size = 1000;
    for (i, chunk) in stations.chunks(batch_size).enumerate() {
        diesel::insert_into(stations_dsl::osm_stations)
            .values(chunk)
            .execute(conn)
            .await?;
        
        if (i + 1) * batch_size % 5000 == 0 || (i + 1) * batch_size >= stations.len() {
            println!("  Inserted {}/{} stations", 
                     std::cmp::min((i + 1) * batch_size, stations.len()), 
                     stations.len());
        }
    }
    
    // Update station count
    diesel::update(imports_dsl::osm_station_imports.filter(imports_dsl::import_id.eq(import_id)))
        .set(imports_dsl::station_count.eq(stations.len() as i32))
        .execute(conn)
        .await?;
    
    println!("Import complete! {} stations imported.", stations.len());
    
    Ok(())
}
