use bincode;
use catenary::routing_common::extract::load_osm_pbf;
use catenary::routing_common::lookup::Lookup;
use catenary::routing_common::ways::RoutingGraph;
use clap::{Parser, Subcommand};
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Parser)]
#[command(
    name = "avens",
    about = "OSM Routing Graph Region Manager",
    version = "1.0"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Extract a local OSM PBF into serialized routing graphs
    Extract {
        /// Input OSM PBF file
        #[arg(long)]
        input: String,

        /// Output directory for serialized graphs
        #[arg(long)]
        output_dir: String,
    },
    /// Print stats for a serialized routing graph
    Info {
        /// Directory containing serialized graphs
        #[arg(long)]
        graph_dir: String,
    },
}

fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Extract { input, output_dir } => {
            println!("Extracting graph from: {}", input);
            let t0 = Instant::now();

            if !std::path::Path::new(input).exists() {
                eprintln!("Error: Input file '{}' does not exist.", input);
                std::process::exit(1);
            }

            // 1. Load PBF and build RoutingGraph
            let graph = load_osm_pbf(input);
            println!(
                "Graph built in {:.2?} ({} nodes, {} ways)",
                t0.elapsed(),
                graph.n_nodes(),
                graph.n_ways()
            );

            // 2. Build Spatial Index (Lookup)
            let t1 = Instant::now();
            let lookup = Lookup::build(&graph);
            println!("Spatial index built in {:.2?}", t1.elapsed());

            // 3. Serialize to disk
            fs::create_dir_all(output_dir).unwrap_or_else(|err| {
                eprintln!("Failed to create output directory {}: {}", output_dir, err);
                std::process::exit(1);
            });

            let t2 = Instant::now();
            let routing_path = PathBuf::from(output_dir).join("routing.bin");
            let lookup_path = PathBuf::from(output_dir).join("lookup.bin");

            let config = bincode::config::standard();
            let routing_bytes = bincode::serde::encode_to_vec(&graph, config)
                .expect("Failed to serialize RoutingGraph");
            fs::write(&routing_path, routing_bytes).expect("Failed to write routing.bin");

            let lookup_bytes =
                bincode::serde::encode_to_vec(&lookup, config).expect("Failed to serialize Lookup");
            fs::write(&lookup_path, lookup_bytes).expect("Failed to write lookup.bin");

            println!(
                "Serialisation complete in {:.2?}. Outputs saved to {}",
                t2.elapsed(),
                output_dir
            );
        }
        Commands::Info { graph_dir } => {
            let routing_path = PathBuf::from(graph_dir).join("routing.bin");

            if !routing_path.exists() {
                eprintln!(
                    "Error: routing graph '{}' does not exist.",
                    routing_path.display()
                );
                std::process::exit(1);
            }

            println!("Graph Dir: {}", graph_dir);

            let t0 = Instant::now();
            let routing_bytes = fs::read(&routing_path).expect("Failed to read routing.bin");
            let config = bincode::config::standard();
            let (graph, _len): (RoutingGraph, usize) =
                bincode::serde::decode_from_slice(&routing_bytes, config)
                    .expect("Failed to deserialize RoutingGraph");

            println!("Loaded in {:.2?}", t0.elapsed());
            println!("Nodes: {}", graph.n_nodes());
            println!("Ways: {:?}", graph.n_ways());
        }
    }
}
