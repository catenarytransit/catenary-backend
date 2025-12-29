use actix_cors::Cors;
use actix_web::{App, HttpResponse, HttpServer, Responder, web};
use clap::Parser;
use std::sync::Arc;

mod config;
mod generator;
mod graph;
mod loader;
mod optimizer;
mod server;
mod tile_gen;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    cmd: Command,
}

#[derive(clap::Subcommand, Debug)]
enum Command {
    /// Serve tiles via HTTP
    Serve {
        #[arg(short, long, default_value = "127.0.0.1")]
        address: String,
        #[arg(short, long, default_value_t = 8080)]
        port: u16,
    },
    /// Export tasks or debug
    Export,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    dotenvy::dotenv().ok();
    env_logger::init();

    // Database Connection
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let manager = diesel::r2d2::ConnectionManager::<diesel::pg::PgConnection>::new(database_url);
    let pool = diesel::r2d2::Pool::builder()
        .build(manager)
        .expect("Failed to create pool");

    // CLI Arguments
    let cli = Args::parse(); // Assuming Args is the new Cli and Command is Commands

    match cli.cmd {
        // Assuming cli.command is cli.cmd
        Command::Serve { port, address } => {
            println!("Starting static tile server on {}:{}", address, port);
            println!("Serving tiles from tiles_output/");

            // Start Server
            HttpServer::new(move || {
                let cors = Cors::permissive();
                App::new()
                    .wrap(cors)
                    // No more graph data needed
                    .configure(server::config)
            })
            .bind((address, port))?
            .run()
            .await
        }
        Command::Export => {
            println!("Starting Static MVT Generation...");

            // Load Graph (same as serve)
            let loader = loader::Loader::new("globeflower_graph.bin".to_string(), pool);
            let mut graph = loader.load_graph().await.map_err(|e: anyhow::Error| {
                std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
            })?;

            println!(
                "Graph loaded. {} edges, {} nodes",
                graph.edges.len(),
                graph.nodes.len()
            );

            // NYC Route Grouping is now handled automatically by loader (group by color)

            // Run Optimizer
            let optimizer = optimizer::Optimizer::new();
            optimizer.optimize(&mut graph);

            println!("Rebuilding spatial indices after optimization...");
            graph.rebuild_indices();

            let generator = generator::Generator::new("tiles_output".to_string());
            generator
                .generate_all(&graph, 5, 17)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

            println!("Generation Complete!");
            Ok(())
        }
    }
}

async fn index() -> impl Responder {
    HttpResponse::Ok().body("Harebell Tile Server")
}
