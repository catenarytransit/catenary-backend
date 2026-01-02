mod clustering;
mod edges;
mod export;
mod geometry_utils;
mod insertion;
mod intercity_split;
mod partition_storage;
mod partitioning;
mod restrictions;
mod support_graph;
mod coord_conversion;
mod validation;
mod collapse_segments;
mod graph_types;
mod node_id_allocator;
mod route_registry;
mod corridor_bundling;
mod station_bundling;

use anyhow::Result;
use catenary::postgres_tools::make_async_pool;
use std::sync::Arc;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    env_logger::init();

    println!("Initializing database connection...");
    let pool = make_async_pool().await.map_err(|e| anyhow::anyhow!(e))?;

    println!("Starting stop clustering...");
    let clusters = clustering::cluster_stops(&pool).await?;

    println!(
        "Found {} clusters. Generating Support Graph (Thesis Ch.3)...",
        clusters.len()
    );

    // 1. Build Support Graph (Geometry merging only)
    let support_edges = support_graph::build_support_graph(&pool).await?;
    println!("Built Support Graph with {} edges.", support_edges.len());

    // 2. Insert Stations into Support Graph
    println!("Inserting stations into Support Graph...");
    let edges = insertion::insert_stations(support_edges, &clusters, &pool).await?;

    println!("Generated {} final edges.", edges.len());

    println!("Inferring Turn Restrictions...");
    let restrictions = restrictions::infer_from_db(&pool, &edges).await?;

    println!("Extracting land masses and exporting...");
    let export_geojson = std::env::var("EXPORT_GEOJSON").is_ok();
    if export_geojson {
        println!("GeoJSON export enabled via EXPORT_GEOJSON env var.");
    }
    export::extract_and_export(&clusters, &edges, restrictions, export_geojson)?;

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
