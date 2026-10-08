use anyhow::{Context, Result};
use clap::Parser;
use diesel::Connection;
use diesel::PgConnection;
use log::info;
use std::path::PathBuf;
use std::time::Instant;

mod export;
#[path = "gtfs2graph_streamed.rs"]
mod gtfs2graph;
mod loom_graph;
mod loom_map_constructor;
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

    /// Initial indexed PostGIS planner tile size. Sparse tiles stay this large.
    #[arg(long, default_value_t = 20.0)]
    planner_tile_degrees: f64,
    /// Dense planner tiles are recursively quartered down to this size.
    #[arg(long, default_value_t = 0.625)]
    planner_min_tile_degrees: f64,
    /// Maximum station/pattern memberships retained for a planner tile.
    #[arg(long, default_value_t = 100_000)]
    planner_row_limit: usize,
}

fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args = Args::parse();
    let started = Instant::now();
    let url = std::env::var("DATABASE_URL").context("DATABASE_URL not set")?;
    let mut conn = PgConnection::establish(&url).context("connect PostgreSQL")?;

    info!(
        "[planner] discovering route_type 0/1 station-connected components with indexed spatial tiles"
    );
    let components = gtfs2graph::discover_components(
        &mut conn,
        gtfs2graph::PlannerConfig {
            tile_degrees: args.planner_tile_degrees,
            min_tile_degrees: args.planner_min_tile_degrees,
            row_limit: args.planner_row_limit,
        },
    )?;

    let cfg = topo::TopoConfig {
        max_aggr_distance: args.max_aggr_distance,
        max_length_dev: args.max_length_dev,
        segment_length: args.segment_length,
        infer_restrictions: !args.no_infer_restrs,
        ..Default::default()
    };

    let mut graph = loom_graph::Graph::default();
    for (index, component) in components.iter().enumerate() {
        let component_started = Instant::now();
        info!(
            "[component {}/{}] loading {} direction patterns",
            index + 1,
            components.len(),
            component.patterns.len()
        );

        // Only this component's stops, route metadata and shapes are materialized.
        let raw = gtfs2graph::build_component(&mut conn, component)?;
        info!(
            "[component {}/{}][gtfs2graph] {} nodes, {} edges, {} lines",
            index + 1,
            components.len(),
            raw.nodes.iter().filter(|n| n.is_some()).count(),
            raw.edges.iter().filter(|e| e.is_some()).count(),
            raw.lines.len()
        );

        // LOOM Chapter 3 topo runs independently because station connectivity
        // guarantees no topological dependency on another work component.
        let component_graph = topo::run(raw, &cfg);
        info!(
            "[component {}/{}][topo] {} nodes, {} edges in {:.2?}",
            index + 1,
            components.len(),
            component_graph.nodes.iter().filter(|n| n.is_some()).count(),
            component_graph.edges.iter().filter(|e| e.is_some()).count(),
            component_started.elapsed()
        );
        graph.append(component_graph);
    }

    export::geojson(&graph, &args.output)?;
    info!(
        "wrote {:?}: {} nodes, {} edges, {} lines in {:.2?}",
        args.output,
        graph.nodes.iter().filter(|n| n.is_some()).count(),
        graph.edges.iter().filter(|e| e.is_some()).count(),
        graph.lines.len(),
        started.elapsed()
    );
    Ok(())
}
