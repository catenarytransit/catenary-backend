mod clustering;
mod collapse_segments;
mod coord_conversion;
mod corridor_bundling;
mod edges;
mod export;
mod geometry_utils;
mod graph_types;
mod insertion;
mod intercity_split;
mod node_id_allocator;
mod osm_collapse;
mod osm_topology;
mod osm_rail_graph;
mod partition_storage;
mod partitioning;
mod regions;
mod restrictions;
mod route_registry;
mod station_spine;
mod station_bundling;
mod support_graph;
mod validation;

use anyhow::Result;
use catenary::postgres_tools::make_async_pool;
use std::sync::Arc;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to OSM railway PBF files (comma-separated).
    /// Can also be set via OSM_RAIL_PBF env var.
    /// If --region is specified, this can be omitted and the default pattern will be used.
    #[arg(long, env = "OSM_RAIL_PBF", value_delimiter = ',')]
    osm_pbf: Option<Vec<String>>,

    /// Process a specific region. Valid options:
    /// north-america, europe, japan, australia, new-zealand, malaysia-singapore-brunei
    #[arg(long)]
    region: Option<String>,

    /// Export result as GeoJSON
    #[arg(long, env = "EXPORT_GEOJSON")]
    export_geojson: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    env_logger::init();

    let args = Args::parse();

    // Determine which region(s) to process
    let region = match &args.region {
        Some(region_str) => Some(
            region_str
                .parse::<regions::Region>()
                .map_err(|e| anyhow::anyhow!(e))?,
        ),
        None => None,
    };

    // Determine OSM PBF paths
    let osm_pbf_paths = match (&args.osm_pbf, &region) {
        (Some(paths), _) => Some(paths.clone()),
        (None, Some(r)) => {
            // Use expected PBF name for this region
            let expected_name = r.expected_pbf_name();
            println!(
                "No --osm-pbf specified, using default for region: {}",
                expected_name
            );
            Some(vec![expected_name])
        }
        (None, None) => None,
    };

    // Initialize OSM rail index for topology validation
    osm_rail_graph::init_global_osm_index(osm_pbf_paths);

    println!("Initializing database connection...");
    let pool = make_async_pool().await.map_err(|e| anyhow::anyhow!(e))?;

    // Get region bbox for filtering (if specified)
    let region_bbox = region.as_ref().map(|r| r.config().bbox);

    println!("Starting stop clustering...");
    let clusters = if let Some(bbox) = region_bbox {
        println!(
            "Filtering to region: {} (bbox: {:?})",
            region.as_ref().unwrap(),
            bbox
        );
        clustering::cluster_stops_in_region(&pool, bbox).await?
    } else {
        clustering::cluster_stops(&pool).await?
    };

    println!(
        "Found {} clusters. Generating Support Graph (Thesis Ch.3)...",
        clusters.len()
    );

    // 1. Build Support Graph (Geometry merging only)
    let support_edges = if let Some(bbox) = region_bbox {
        support_graph::build_support_graph_in_region(&pool, bbox).await?
    } else {
        support_graph::build_support_graph(&pool).await?
    };
    println!("Built Support Graph with {} edges.", support_edges.len());

    // 2. Insert Stations into Support Graph
    println!("Inserting stations into Support Graph...");
    let edges = insertion::insert_stations(support_edges, &clusters, &pool).await?;

    println!("Generated {} final edges.", edges.len());

    println!("Inferring Turn Restrictions...");
    let restrictions = restrictions::infer_from_db(&pool, &edges).await?;

    println!("Extracting land masses and exporting...");
    if args.export_geojson {
        println!("GeoJSON export enabled via flag/env var.");
    }

    // Export with region suffix if specified
    export::extract_and_export_region(
        &clusters,
        &edges,
        restrictions,
        region.as_ref(),
        args.export_geojson,
    )?;

    println!("Done.");

    // Debug: Print some clusters
    for (i, cluster) in clusters.iter().take(5).enumerate() {
        println!(
            "Cluster {}: {} stops, Centroid: ({}, {})",
            i,
            cluster.stops.len(),
            cluster.centroid.x,
            cluster.centroid.y
        );

        for stop in &cluster.stops {
            println!(
                "   - [{}] {} ({:?})",
                stop.gtfs_id,
                stop.name.as_deref().unwrap_or("Unnamed"),
                stop.route_types
            );
        }
    }

    Ok(())
}
