mod clustering;
mod edges;
mod export;
mod geometry_utils;

use anyhow::Result;
use catenary::postgres_tools::make_async_pool;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    env_logger::init();

    println!("Initializing database connection...");
    let pool = make_async_pool().await.map_err(|e| anyhow::anyhow!(e))?;

    println!("Starting stop clustering...");
    let clusters = clustering::cluster_stops(&pool).await?;

    println!("Found {} clusters. Generating edges...", clusters.len());

    let edges = edges::generate_edges(&pool, &clusters).await?;
    println!("Generated {} raw edges.", edges.len());
    
    println!("Extracting land masses and exporting...");
    export::extract_and_export(&clusters, &edges)?;
    
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
