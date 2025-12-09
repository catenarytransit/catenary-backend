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
pub mod pathfinding;
pub mod reduce_borders;
pub mod utils;

pub mod cluster_global;
pub mod extract;
pub mod rebuild_patterns;
pub mod station_matching;
pub mod trip_based;
pub mod update_gtfs;

#[cfg(test)]
pub mod test_trip_based;
#[cfg(test)]
pub mod test_trip_based_profile;

use crate::clustering::merge_based_clustering;
use crate::connectivity::{
    compute_border_patterns, compute_global_patterns, compute_local_patterns_for_partition,
};
use crate::osm::{ChunkCache, find_osm_link};
use crate::reduce_borders::reduce_borders_by_merging;
use crate::utils::haversine_distance;
use utils::{ProcessedPattern, calculate_service_mask, reindex_deltas};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(clap::Subcommand, Debug)]
enum Commands {
    /// Extract data from DB and create shards
    Extract {
        #[arg(long)]
        chateau: Option<String>,
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        osm_chunks: Option<String>,
    },
    /// Cluster the graph
    Cluster {
        #[arg(long)]
        output: PathBuf,
    },
    /// Update GTFS data for a specific chateau
    UpdateGtfs {
        #[arg(long)]
        chateau: String,
        #[arg(long)]
        output: PathBuf,
    },
    /// Rebuild patterns for affected partitions
    RebuildPatterns {
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        partitions: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    if let Err(e) = rustix::process::nice(10) {
        eprintln!("Failed to set priority: {}", e);
    } else {
        println!("Process priority set to low (nice +10).");
    }

    // Initialize logging
    tracing_subscriber::fmt::init();

    // Connect to DB
    let pool = Arc::new(make_async_pool().await.map_err(|e| anyhow::anyhow!(e))?);

    // Create output directory (if needed by subcommands, they will do it)

    match args.command {
        Some(Commands::Extract {
            chateau,
            output,
            osm_chunks,
        }) => {
            tokio::fs::create_dir_all(&output)
                .await
                .context("Failed to create output dir")?;
            let chateaux_list: Vec<String> = chateau
                .unwrap_or_default()
                .split(',')
                .map(|s| s.to_string())
                .filter(|s| !s.is_empty())
                .collect();
            extract::run_extraction(pool, chateaux_list, &output).await?;
        }
        Some(Commands::Cluster { output }) => {
            tokio::fs::create_dir_all(&output)
                .await
                .context("Failed to create output dir")?;
            cluster_global::run_global_clustering(&output)?;
        }
        Some(Commands::UpdateGtfs { chateau, output }) => {
            tokio::fs::create_dir_all(&output)
                .await
                .context("Failed to create output dir")?;
            update_gtfs::run_update_gtfs(pool, chateau, &output).await?;
        }
        Some(Commands::RebuildPatterns { output, partitions }) => {
            tokio::fs::create_dir_all(&output)
                .await
                .context("Failed to create output dir")?;
            let partitions_vec =
                partitions.map(|s| s.split(',').filter_map(|p| p.parse::<u32>().ok()).collect());
            rebuild_patterns::run_rebuild_patterns(pool, &output, partitions_vec).await?;
        }
        None => {
            anyhow::bail!("No command specified. Use --help for available commands.");
        }
    }

    Ok(())
}
