use anyhow::{Context, Result};
use clap::Parser;
use diesel::Connection;
use diesel::PgConnection;
use log::info;
use std::path::PathBuf;
use std::time::Instant;

mod export;
mod gtfs2graph;
mod loom_graph;
mod topo;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "LOOM Chapter-3 gtfs2graph + topo over Catenary PostgreSQL GTFS"
)]
struct Args {
    #[arg(long, default_value = "globeflower.geojson")]
    output: PathBuf,
    #[arg(long, default_value_t = 50.0)]
    max_aggr_distance: f64,
    #[arg(long, default_value_t = 5.0)]
    segment_length: f64,
    #[arg(long, default_value_t = 500.0)]
    max_length_dev: f64,
    #[arg(long, default_value_t = false)]
    no_infer_restrs: bool,
}

fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args = Args::parse();
    let started = Instant::now();
    let url = std::env::var("DATABASE_URL").context("DATABASE_URL not set")?;
    let mut conn = PgConnection::establish(&url).context("connect PostgreSQL")?;

    info!("[gtfs2graph] reading non-bus GTFS from PostgreSQL");
    let raw = gtfs2graph::build(&mut conn)?;
    info!(
        "[gtfs2graph] {} nodes, {} edges, {} lines",
        raw.nodes.iter().filter(|n| n.is_some()).count(),
        raw.edges.iter().filter(|e| e.is_some()).count(),
        raw.lines.len()
    );

    let cfg = topo::TopoConfig {
        max_aggr_distance: args.max_aggr_distance,
        max_length_dev: args.max_length_dev,
        segment_length: args.segment_length,
        infer_restrictions: !args.no_infer_restrs,
        ..Default::default()
    };
    info!("[topo] constructing free line graph");
    let graph = topo::run(raw, &cfg);
    info!(
        "[topo] {} nodes, {} edges",
        graph.nodes.iter().filter(|n| n.is_some()).count(),
        graph.edges.iter().filter(|e| e.is_some()).count()
    );

    export::geojson(&graph, &args.output)?;
    info!("wrote {:?} in {:.2?}", args.output, started.elapsed());
    Ok(())
}
