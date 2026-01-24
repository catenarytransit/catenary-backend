use anyhow::Result;
use catenary::postgres_tools::make_async_pool;
use clap::Parser;
use log::{debug, error, info, warn};
use std::path::PathBuf;

mod corridor;
mod export;
mod geometry_utils;
mod gtfs_loader;
mod map_matcher;
mod osm_loader;
mod osm_types;
mod regions;
mod restrictions;
mod station;
mod support_graph;

use ahash::HashMapExt;
use corridor::{CorridorBuilder, CorridorConfig};
use gtfs_loader::GtfsLoader;
use osm_loader::OsmRailIndex;
use osm_types::{Line, LineId, LineOcc};
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

    /// Skip GTFS database queries (OSM-only mode)
    #[arg(long, default_value = "false")]
    osm_only: bool,
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

    // 1. Load OSM data
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

    // 2. Load GTFS data & Map-Match to OSM (Pre-pass)
    // We do this BEFORE corridor building so we can use route info for gating merges.
    let (lines, stop_clusters, edge_to_routes, matches) = if args.osm_only {
        info!("OSM-only mode: skipping GTFS database queries");
        (
            Vec::new(),
            Vec::new(),
            ahash::HashMap::<crate::osm_types::AtomicEdgeId, Vec<LineId>>::new(),
            Vec::new(),
        )
    } else {
        load_and_match_gtfs_to_osm(&config, &osm_index)?
    };

    // 3. Build corridor clusters to disk (streaming)
    let corridor_config = corridor::CorridorConfig {
        max_hausdorff_m: 25.0,
        max_angle_diff_deg: 15.0,
        min_overlap_ratio: 0.3,
        centerline_sample_interval: 10.0,
    };

    // Pass edge_to_routes to builder
    let corridor_builder = corridor::CorridorBuilder::new(
        &osm_index,
        corridor_config,
        Some(&edge_to_routes), // Route gating
    );

    // Use temp directory for corridor tiles
    let tile_dir = args.output_dir.join(".corridor_tiles");
    let tile_size_deg = 0.1; // ~11km tiles
    let disk_index = corridor_builder.build_corridors_to_disk(&tile_dir, tile_size_deg)?;
    info!(
        "Built {} corridor clusters across {} tiles",
        disk_index.total_corridors,
        disk_index.tiles.len()
    );

    // 4. Build support graph (streaming from disk)
    let support_config = support_graph::SupportGraphConfig {
        sample_interval_m: 25.0,
        merge_threshold_m: 50.0,
        max_angle_diff_deg: 45.0, // Used for logical filtering, not Pass 1 planarization
        convergence_threshold: 0.002,
        tile_size_deg,
        adaptive_sampling: true,
    };
    let mut support_graph = support_graph::SupportGraph::from_disk_corridors(
        &disk_index,
        &support_config,
        Some(&edge_to_routes),
    )?;

    // Enrich with station stops
    support_graph.enrich_with_stations(&osm_index.pt_index, &support_config);

    // 5. Apply Lines to Support Graph
    // Since we propagated routes through CorridorCluster -> SupportEdge (TODO), we might not need re-matching.
    // However, if SupportEdge doesn't natively store "LineId" list yet, we might traverse.
    // Ideally SupportEdge stores `routes: Vec<LineId>`.
    // For now, let's assume we need to populate the `lines` output structure using the support graph data.
    // If support graph edges have `routes`, we can reconstruct `line.edges`.
    // Or we can just re-match or use the mapping if we maintained it.

    // For this step, let's assume `SupportGraph` will be updated to hold route info.
    // But `export_geojson` expects `lines: Vec<Line>` where `Line` has no edges usually?
    // Wait, `Line` struct in `osm_types` likely doesn't hold geometry, just metadata.
    // `export_geojson` usually iterates `support_graph` edges and sees which lines are on them?
    // Actually `export_geojson` takes `&Vec<Line>` just for metadata (colors etc).
    // The geometry comes from `support_graph`.

    // BUT we need `matched_sequences` for restrictions?
    // User requested "provenance... infer per-route connectivity...".
    // Let's assume for now we populate restriction-inference data from SupportEdge info.

    // Clean up corridor tiles
    if let Err(e) = disk_index.cleanup() {
        warn!("Failed to clean up corridor tiles: {}", e);
    }
    info!(
        "Support graph: {} nodes, {} edges",
        support_graph.nodes.len(),
        support_graph.edges.len()
    );

    // Insert stations
    info!("Inserting stations...");
    let station_config = StationConfig::default();
    let mut station_handler = StationHandler::new(&mut support_graph, station_config);
    let inserted_stations = station_handler.insert_stations(&stop_clusters);
    info!("Inserted {} stations", inserted_stations.len());

    // Build turn restrictions
    let mut restrictions = TurnRestrictions::default();

    if !matches.is_empty() {
        info!("Building atomic -> support edge index for restriction inference...");
        // Index: AtomicEdgeId -> Vec<SupportEdgeId>
        let mut atomic_to_support: ahash::HashMap<
            crate::osm_types::AtomicEdgeId,
            Vec<support_graph::SupportEdgeId>,
        > = ahash::HashMap::new();

        // 1. Map Corridor -> Atomic Edges

        // We can iterate the disk index once to build `CorridorId -> Vec<AtomicEdgeId>`.
        let mut corridor_to_atomic: ahash::HashMap<
            corridor::CorridorId,
            Vec<crate::osm_types::AtomicEdgeId>,
        > = ahash::HashMap::new();

        info!("Reading corridors to build atomic mapping...");
        for tile_key in disk_index.sorted_tile_keys() {
            if let Ok(corridors) = disk_index.read_tile(tile_key) {
                for c in corridors {
                    corridor_to_atomic.insert(c.id, c.member_edges);
                }
            }
        }

        // 2. Map Support -> Atomic via Corridor
        for edge in support_graph.edges.values() {
            for cid in &edge.source_corridors {
                if let Some(atomics) = corridor_to_atomic.get(cid) {
                    for atomic_id in atomics {
                        atomic_to_support
                            .entry(*atomic_id)
                            .or_default()
                            .push(edge.id);
                    }
                }
            }
        }

        // Infer restrictions
        restrictions.record_from_matches(&matches, &atomic_to_support, &support_graph);
    }

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

/// Load GTFS data and match to OSM Atomic Edges (Pre-pass)
fn load_and_match_gtfs_to_osm(
    config: &regions::RegionConfig,
    osm_index: &OsmRailIndex,
) -> Result<(
    Vec<Line>,
    Vec<station::StopCluster>,
    ahash::HashMap<crate::osm_types::AtomicEdgeId, Vec<LineId>>,
    Vec<crate::map_matcher::MatchedShape>,
)> {
    use crate::map_matcher::{MapMatcher, MatchConfig};

    // Connect to database
    let database_url = std::env::var("DATABASE_URL")
        .map_err(|_| anyhow::anyhow!("DATABASE_URL environment variable not set"))?;

    info!("Connecting to database...");
    let manager = diesel::r2d2::ConnectionManager::<diesel::pg::PgConnection>::new(&database_url);
    let pool = diesel::r2d2::Pool::builder()
        .build(manager)
        .map_err(|e| anyhow::anyhow!("Failed to create pool: {}", e))?;

    // Load GTFS data
    let gtfs_loader = GtfsLoader::new(pool, config);

    info!("Loading GTFS shapes...");
    let (lines, shapes) = gtfs_loader.load_rail_shapes()?;
    info!("Loaded {} lines, {} shapes", lines.len(), shapes.len());

    info!("Loading GTFS stops...");
    let gtfs_stops = gtfs_loader.load_rail_stops()?;

    // Cluster stops
    let stop_clusters = cluster_gtfs_stops(&gtfs_stops);
    info!("Created {} stop clusters", stop_clusters.len());

    // Map Match to OSM
    info!("Map-matching shapes to OSM Atomic Edges...");
    let mut edge_to_routes: ahash::HashMap<crate::osm_types::AtomicEdgeId, Vec<LineId>> =
        ahash::HashMap::new();
    let mut matches = Vec::new();

    let matcher_config = MatchConfig {
        snap_radius_m: 50.0,
        z_class_change_penalty: 500.0,
        mode_mismatch_penalty: 1000.0,
        max_gap_m: 1000.0,
    };
    let matcher = MapMatcher::new(osm_index, matcher_config);

    // Build line lookup map for filtering
    let line_map: ahash::HashMap<LineId, &Line> = lines.iter().map(|l| (l.id.clone(), l)).collect();

    let mut processed = 0;
    let total = shapes.len();

    for shape in shapes {
        for route_id in &shape.route_ids {
            let line_id = LineId::new(shape.chateau.clone(), route_id.clone());

            // Mode mapping
            let mode = match shape.route_type {
                0 => crate::osm_types::RailMode::Tram,
                1 => crate::osm_types::RailMode::Subway,
                2 => crate::osm_types::RailMode::Rail,
                5 | 7 => crate::osm_types::RailMode::Funicular,
                12 => crate::osm_types::RailMode::Monorail,
                _ => crate::osm_types::RailMode::Rail,
            };

            // Rail filtering logic (exclude general rail, allow exceptions)
            if mode == crate::osm_types::RailMode::Rail {
                let mut allowed = false;
                if let Some(line) = line_map.get(&line_id) {
                    let c = line_id.chateau.as_str();
                    // Check simple chateau exceptions
                    if c == "metrolinktrains"
                        || c == "exo~reseaudetransportmetropolitain"
                        || c == "gotransit"
                        || c == "upexpress"
                        || c == "île~de~france~mobilités"
                        || c == "longislandrailroad"
                        || c == "metro~northrailroad"
                        || c == "renfecercanias"
                        || c == "fgc"
                        || c == "utahtransitauthority"
                        || c == "metra"
                        || c == "northernindianacommutertransportationdistrict"
                        || c == "jr~east"
                        || c == "mir~tsukuba"
                        || c == "new-south-wales"
                        || c == "ptv"
                        || c == "translink-queensland-au"
                    {
                        allowed = true;
                    }
                    // Check Deutschland / DB Regio S-Bahn / Switzerland
                    else if (c == "deutschland" || c == "dbregioag" || c == "schweiz")
                        && line.label.starts_with('S')
                    {
                        allowed = true;
                    }
                    // Check London Overground
                    else if c == "nationalrailuk" && line.agency_id.as_deref() == Some("LO") {
                        allowed = true;
                    }
                }

                if !allowed {
                    continue;
                }
            }

            if let Some(matched) = matcher.match_shape(line_id.clone(), &shape.geometry, mode) {
                for edge_id in &matched.edges {
                    edge_to_routes
                        .entry(*edge_id)
                        .or_default()
                        .push(line_id.clone());
                }
                matches.push(matched);
            }
        }
        processed += 1;
        if processed % 100 == 0 {
            info!("Matched {}/{} shapes", processed, total);
        }
    }

    Ok((lines, stop_clusters, edge_to_routes, matches))
}

/// Cluster GTFS stops by proximity
fn cluster_gtfs_stops(stops: &[gtfs_loader::GtfsStop]) -> Vec<station::StopCluster> {
    // Simple clustering: group stops within 100m of each other
    let mut clusters: Vec<station::StopCluster> = Vec::new();
    let mut assigned: std::collections::HashSet<usize> = std::collections::HashSet::new();

    for (i, stop) in stops.iter().enumerate() {
        if assigned.contains(&i) {
            continue;
        }

        let mut cluster_stops = vec![i];
        assigned.insert(i);

        // Find nearby stops
        for (j, other) in stops.iter().enumerate().skip(i + 1) {
            if assigned.contains(&j) {
                continue;
            }
            let dist = geometry_utils::polyline_length(&[stop.position, other.position]);
            if dist < 100.0 {
                cluster_stops.push(j);
                assigned.insert(j);
            }
        }

        // Calculate centroid
        let sum: (f64, f64) = cluster_stops
            .iter()
            .map(|&idx| stops[idx].position)
            .fold((0.0, 0.0), |(a, b), (x, y)| (a + x, b + y));
        let n = cluster_stops.len() as f64;
        let centroid = (sum.0 / n, sum.1 / n);

        // Use first stop's name as cluster name
        let name = stops[cluster_stops[0]].name.clone();

        clusters.push(station::StopCluster {
            centroid,
            label: name.unwrap_or_else(|| format!("Cluster {}", clusters.len())),
            stop_ids: cluster_stops
                .iter()
                .map(|&idx| stops[idx].stop_id.clone())
                .collect(),
        });
    }

    clusters
}
