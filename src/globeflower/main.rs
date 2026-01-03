use anyhow::Result;
use clap::Parser;
use log::{debug, error, info, warn};
use std::path::PathBuf;

mod corridor;
mod export;
mod geometry_utils;
mod map_matcher;
mod osm_loader;
mod osm_types;
mod regions;
mod restrictions;
mod station;
mod support_graph;

use corridor::{CorridorBuilder, CorridorConfig, CorridorIndex};
use osm_loader::OsmRailIndex;
use osm_types::{Line, LineId};
use regions::Region;
use restrictions::TurnRestrictions;
use station::{StationConfig, StationHandler, cluster_stops};
use support_graph::{SupportGraph, SupportGraphConfig};

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Build overlap-free line graphs from OSM + GTFS"
)]
struct Args {
    /// Region to process (europe, north-america, japan, etc.)
    #[arg(long)]
    region: String,

    /// Directory containing OSM PBF files
    #[arg(long, default_value = ".")]
    osm_dir: PathBuf,

    /// Output directory for generated files
    #[arg(long, default_value = ".")]
    output_dir: PathBuf,
}

fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    env_logger::init();

    let args = Args::parse();
    info!("Globeflower starting for region: {}", args.region);

    // Parse region
    let region: Region = args
        .region
        .parse()
        .map_err(|e: String| anyhow::anyhow!(e))?;
    let config = region.config();
    info!(
        "Processing region: {} ({:?})",
        config.display_name, config.bbox
    );

    // Load OSM data
    let osm_path = args.osm_dir.join(region.expected_pbf_name());
    info!("Loading OSM from {:?}", osm_path);

    if !osm_path.exists() {
        error!("OSM file not found: {:?}", osm_path);
        error!("Expected file: {}", region.expected_pbf_name());
        return Err(anyhow::anyhow!("OSM PBF file not found"));
    }

    let osm_index = OsmRailIndex::load_from_pbf(&osm_path)?;
    info!(
        "OSM loaded: {} nodes, {} ways, {} atomic edges",
        osm_index.nodes.len(),
        osm_index.ways.len(),
        osm_index.edges.len()
    );

    // Build corridor clusters
    info!("Building corridor clusters...");
    let corridor_config = CorridorConfig::default();
    let corridor_builder = CorridorBuilder::new(&osm_index, corridor_config);
    let corridors = corridor_builder.build_corridors();
    let corridor_index = CorridorIndex::from_clusters(corridors);
    info!("Built {} corridor clusters", corridor_index.corridors.len());

    // Build support graph
    info!("Building support graph...");
    let support_config = SupportGraphConfig::default();
    let mut support_graph = SupportGraph::from_corridors(&corridor_index, &support_config);
    info!(
        "Support graph: {} nodes, {} edges",
        support_graph.nodes.len(),
        support_graph.edges.len()
    );

    // For now, no GTFS data integration - just OSM skeleton
    // TODO: Add database queries for GTFS shapes/stops when needed
    let lines: Vec<Line> = Vec::new();
    let stop_clusters = Vec::new();
    let matched_sequences: Vec<(LineId, Vec<support_graph::SupportEdgeId>)> = Vec::new();

    // Insert stations (would need GTFS data for real stops)
    info!("Inserting stations...");
    let station_config = StationConfig::default();
    let mut station_handler = StationHandler::new(&mut support_graph, station_config);
    let inserted_stations = station_handler.insert_stations(&stop_clusters);
    info!("Inserted {} stations", inserted_stations.len());

    // Build turn restrictions
    info!("Building turn restrictions...");
    let restrictions =
        TurnRestrictions::build_from_edge_sequences(&matched_sequences, &support_graph);

    // Export outputs
    std::fs::create_dir_all(&args.output_dir)?;

    // GeoJSON export
    let geojson_path = args.output_dir.join(region.output_geojson_name());
    export::export_geojson(&support_graph, &lines, &restrictions, &geojson_path)?;
    info!("Exported GeoJSON to {:?}", geojson_path);

    // Binary export
    let binary_path = args.output_dir.join(region.output_graph_name());
    export::export_binary(&support_graph, &restrictions, &binary_path)?;
    info!("Exported binary to {:?}", binary_path);

    info!("Globeflower complete!");
    Ok(())
}
