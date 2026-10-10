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
mod loom_builder_simplify;
mod loom_cpp_topo;
mod loom_graph;
mod loom_map_constructor;
mod loom_polyline;
mod loom_restr_inferrer;
mod loom_shape_alignment;
mod loom_trip_segments;
mod topo;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "LOOM Chapter-3 gtfs2graph + topo over Catenary PostgreSQL GTFS"
)]
struct Args {
    /// GeoJSON output path (parent directories are created if needed).
    #[arg(short = 'o', long, default_value = "globeflower.geojson")]
    output: PathBuf,
    /// Limit GTFS processing to one or more comma-separated chateau IDs.
    /// Omit to process all production tram/metro routes.
    #[arg(long, visible_alias = "chateaux", value_name = "ID[,ID...]")]
    chateau: Option<String>,
    #[arg(long, default_value_t = 50.0)]
    max_aggr_distance: f64,
    #[arg(long, default_value_t = 5.0)]
    segment_length: f64,
    #[arg(long, default_value_t = 500.0)]
    max_length_dev: f64,
    #[arg(long, default_value_t = false)]
    no_infer_restrs: bool,
    /// C++ gtfs2graph --prune-threshold; 0.0 disables rare-service pruning.
    #[arg(long, default_value_t = 0.0)]
    prune_threshold: f64,

    /// Initial indexed PostGIS planner tile size. Sparse tiles stay this large.
    #[arg(long, default_value_t = 20.0)]
    planner_tile_degrees: f64,
    /// Dense planner tiles are recursively quartered down to this size.
    #[arg(long, default_value_t = 0.625)]
    planner_min_tile_degrees: f64,
    /// Maximum station/pattern memberships retained for a planner tile.
    #[arg(long, default_value_t = 100_000)]
    planner_row_limit: usize,
    /// LOOM geographic proximity for distConnectedComponents (Web Mercator metres).
    #[arg(long, default_value_t = 10_000.0)]
    connected_comp_distance: f64,
}

/// Parse the optional CLI filter once, before any database queries.
/// Preserve case because chateau IDs are PostgreSQL text identifiers.
fn parse_chateaux(raw: Option<&str>) -> Result<Option<Vec<String>>> {
    raw.map(|value| {
        let mut ids = Vec::<String>::new();
        for part in value.split(',') {
            let id = part.trim();
            anyhow::ensure!(
                !id.is_empty(),
                "--chateau needs nonempty comma-separated IDs (e.g. --chateau A,B)"
            );
            if !ids.iter().any(|existing| existing == id) {
                ids.push(id.to_owned());
            }
        }
        Ok(ids)
    })
    .transpose()
}

fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args = Args::parse();
    let chateaux = parse_chateaux(args.chateau.as_deref())?;
    if let Some(selected) = &chateaux {
        info!("[planner] chateau filter: {}", selected.join(", "));
    }
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
            connected_comp_distance: args.connected_comp_distance,
        },
        chateaux.as_deref(),
    )?;

    let cfg = topo::TopoConfig {
        max_aggr_distance: args.max_aggr_distance,
        max_length_dev: args.max_length_dev,
        segment_length: args.segment_length,
        infer_restrictions: !args.no_infer_restrs,
        ..Default::default()
    };

    // Stream each independent component to disk; never retain a world-sized graph.
    let mut writer = export::GeoJsonWriter::create(&args.output)?;
    for (index, component) in components.iter().enumerate() {
        let component_started = Instant::now();
        info!(
            "[component {}/{}] loading {} direction patterns",
            index + 1,
            components.len(),
            component.patterns.len()
        );

        // Only this component's stops, route metadata and shapes are materialized.
        let raw = gtfs2graph::build_component(&mut conn, component, args.prune_threshold)?;
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
        writer.write_component(&component_graph)?;
        // Component graph and all of its geometry can now be released.
        drop(component_graph);
    }

    let counts = writer.finish()?;
    info!(
        "wrote {:?}: {} nodes, {} edges, {} lines in {:.2?}",
        args.output,
        counts.nodes,
        counts.edges,
        counts.lines,
        started.elapsed()
    );
    Ok(())
}

#[cfg(test)]
mod cli_tests {
    use super::*;

    #[test]
    fn single_chateau_and_custom_output() {
        let args = Args::try_parse_from([
            "globeflower",
            "--chateau",
            "regionaltransportationdistrict",
            "--output",
            "testing/denver.geojson",
        ])
        .unwrap();
        assert_eq!(
            parse_chateaux(args.chateau.as_deref()).unwrap(),
            Some(vec!["regionaltransportationdistrict".to_owned()])
        );
        assert_eq!(args.output, PathBuf::from("testing/denver.geojson"));
    }

    #[test]
    fn comma_separated_chateaux_are_trimmed_and_deduplicated() {
        let args = Args::try_parse_from([
            "globeflower",
            "--chateaux",
            "first, second,first",
            "-o",
            "testing/two.geojson",
        ])
        .unwrap();
        assert_eq!(
            parse_chateaux(args.chateau.as_deref()).unwrap(),
            Some(vec!["first".to_owned(), "second".to_owned()])
        );
        assert_eq!(args.output, PathBuf::from("testing/two.geojson"));
    }

    #[test]
    fn absent_filter_means_all_chateaux() {
        let args = Args::try_parse_from(["globeflower"]).unwrap();
        assert_eq!(parse_chateaux(args.chateau.as_deref()).unwrap(), None);
        assert_eq!(args.output, PathBuf::from("globeflower.geojson"));
    }

    #[test]
    fn empty_chateau_tokens_are_rejected() {
        for input in ["", ",first", "first,", "first,,second", " , "] {
            assert!(parse_chateaux(Some(input)).is_err(), "accepted {input:?}");
        }
    }
}
