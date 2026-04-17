use catenary::routing_common::extract::extract_osm_graph;
use catenary::routing_common::lookup::Lookup;
use catenary::routing_common::ways::RoutingGraph;
use clap::{Parser, Subcommand};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

#[derive(Deserialize, Debug)]
struct Config {
    global: GlobalConfig,
    regions: Vec<RegionConfig>,
}

#[derive(Deserialize, Debug)]
struct GlobalConfig {
    tmp_dir: PathBuf,
    output_dir: PathBuf,
}

#[derive(Deserialize, Debug)]
struct RegionConfig {
    name: String,
    url: String,
}

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
    /// Pull an OSM PBF directly from a given URL and extract it into a routing graph
    Pull {
        /// Download URL (e.g. Geofabrik download link for a .osm.pbf file)
        #[arg(long)]
        url: String,

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
    /// Batch process OSM PBFs from a TOML configuration file
    Batch {
        /// Path to TOML configuration file
        #[arg(long, default_value = "regions.toml")]
        config: PathBuf,
    },
}

fn extract_to_dir(input: &Path, output_dir: &Path) {
    println!("Extracting graph from: {}", input.display());
    let t0 = Instant::now();

    if !input.exists() {
        eprintln!("Error: Input file '{}' does not exist.", input.display());
        std::process::exit(1);
    }

    let routing_path = output_dir.join("routing.bin");
    extract_osm_graph(input.to_string_lossy().as_ref(), &routing_path);

    let mmap = std::sync::Arc::new(
        unsafe {
            memmap2::Mmap::map(
                &std::fs::File::open(&routing_path).expect("Failed to open graph map"),
            )
        }
        .unwrap(),
    );
    let graph = RoutingGraph::load(mmap);

    println!(
        "Graph built in {:.2?} ({} nodes, {} ways)",
        t0.elapsed(),
        graph.n_nodes(),
        graph.n_ways()
    );

    let mut min_lat = f64::MAX;
    let mut max_lat = f64::MIN;
    let mut min_lon = f64::MAX;
    let mut max_lon = f64::MIN;

    for pos in graph.node_positions() {
        min_lat = f64::min(min_lat, pos.lat());
        max_lat = f64::max(max_lat, pos.lat());
        min_lon = f64::min(min_lon, pos.lng());
        max_lon = f64::max(max_lon, pos.lng());
    }

    let manifest = serde_json::json!({
        "min_lat": min_lat,
        "max_lat": max_lat,
        "min_lon": min_lon,
        "max_lon": max_lon
    });

    // 2. Build Spatial Index (Lookup)
    let t1 = Instant::now();
    let lookup = Lookup::build(&graph);
    println!("Spatial index built in {:.2?}", t1.elapsed());

    // 3. Serialize to disk
    fs::create_dir_all(output_dir).unwrap_or_else(|err| {
        eprintln!(
            "Failed to create output directory {}: {}",
            output_dir.display(),
            err
        );
        std::process::exit(1);
    });

    let t2 = Instant::now();
    let lookup_path = output_dir.join("lookup.bin");
    let manifest_path = output_dir.join("manifest.json");

    let config = bincode::config::standard();
    let lookup_bytes =
        bincode::serde::encode_to_vec(&lookup, config).expect("Failed to serialize Lookup");
    fs::write(&lookup_path, lookup_bytes).expect("Failed to write lookup.bin");

    fs::write(
        &manifest_path,
        serde_json::to_string_pretty(&manifest).unwrap(),
    )
    .expect("Failed to write manifest.json");

    println!(
        "Serialisation complete in {:.2?}. Outputs saved to {}",
        t2.elapsed(),
        output_dir.display()
    );
}

fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Extract { input, output_dir } => {
            extract_to_dir(Path::new(input), Path::new(output_dir));
        }
        Commands::Pull { url, output_dir } => {
            println!("Downloading from: {}", url);
            let t0 = Instant::now();
            let mut response = reqwest::blocking::get(url).unwrap_or_else(|e| {
                eprintln!("Failed to download URL: {}", e);
                std::process::exit(1);
            });

            let temp_dir = std::env::temp_dir();
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let temp_file = temp_dir.join(format!("avens_download_{}.osm.pbf", timestamp));

            let mut file =
                std::fs::File::create(&temp_file).expect("Failed to create temporary file");
            response
                .copy_to(&mut file)
                .expect("Failed to write to temporary file");
            println!("Downloaded in {:.2?}", t0.elapsed());

            extract_to_dir(&temp_file, Path::new(output_dir));

            // cleanup
            let _ = std::fs::remove_file(&temp_file);
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
            let mmap = std::sync::Arc::new(
                unsafe {
                    memmap2::Mmap::map(
                        &std::fs::File::open(&routing_path).expect("Failed to open graph map"),
                    )
                }
                .unwrap(),
            );
            let graph = RoutingGraph::load(mmap);

            println!("Loaded in {:.2?}", t0.elapsed());
            println!("Nodes: {}", graph.n_nodes());
            println!("Ways: {}", graph.n_ways());
        }
        Commands::Batch { config } => {
            let config_content = fs::read_to_string(config).unwrap_or_else(|e| {
                eprintln!("Failed to read config file {}: {}", config.display(), e);
                std::process::exit(1);
            });
            let config_data: Config = toml::from_str(&config_content).unwrap_or_else(|e| {
                eprintln!("Failed to parse config file: {}", e);
                std::process::exit(1);
            });

            fs::create_dir_all(&config_data.global.tmp_dir).unwrap_or_else(|e| {
                eprintln!("Failed to create local tmp_dir: {}", e);
                std::process::exit(1);
            });
            fs::create_dir_all(&config_data.global.output_dir).unwrap_or_else(|e| {
                eprintln!("Failed to create local output_dir: {}", e);
                std::process::exit(1);
            });

            for region in config_data.regions {
                println!("Processing region: {}", region.name);

                let filename = region.url.split('/').last().unwrap_or("download.osm.pbf");
                let temp_file = config_data.global.tmp_dir.join(filename);

                println!("Downloading from: {}", region.url);
                let t0 = Instant::now();
                let mut response = reqwest::blocking::get(&region.url).unwrap_or_else(|e| {
                    eprintln!("Failed to download URL for {}: {}", region.name, e);
                    std::process::exit(1);
                });

                let mut file = std::fs::File::create(&temp_file).unwrap_or_else(|e| {
                    panic!(
                        "Failed to create temporary file at {}: {}",
                        temp_file.display(),
                        e
                    )
                });
                response.copy_to(&mut file).unwrap_or_else(|e| {
                    panic!(
                        "Failed to write to temporary file at {}: {}",
                        temp_file.display(),
                        e
                    )
                });
                println!("Downloaded in {:.2?}", t0.elapsed());

                let output_dir = config_data.global.output_dir.join(&region.name);
                extract_to_dir(&temp_file, &output_dir);
                println!("Finished processing region: {}\n", region.name);
            }
        }
    }
}
