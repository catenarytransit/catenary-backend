mod graph_loader;
mod hydrator;
mod osm_router;
mod query_graph;
mod router;

#[cfg(test)]
mod repro_test;
#[cfg(test)]
mod test_direct_connections_intermediate;
#[cfg(test)]
mod test_graph_construction;
#[cfg(test)]
mod test_multi_target;

use crate::graph_loader::GraphManager;
use crate::hydrator::Hydrator;
use crate::router::Router;
use catenary::postgres_tools::make_async_pool;
use catenary::routing_common::api;
use catenary::routing_common::api::EdelweissService;
use futures::StreamExt;
use std::sync::Arc;
use tarpc::server::{self, Channel};
use tokio::net::TcpListener;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[derive(Clone)]
struct EdelweissServer {
    graph: Arc<GraphManager>,
    hydrator: Arc<Hydrator>,
}

impl EdelweissService for EdelweissServer {
    async fn route(
        self,
        _ctx: tarpc::context::Context,
        req: api::RoutingRequest,
    ) -> api::RoutingResult {
        let router = Router::new(&self.graph);
        let mut result = router.route(&req);

        // Collect IDs for hydration
        let mut stop_ids = Vec::new();
        let mut route_ids = Vec::new();

        for itinerary in &result.itineraries {
            for leg in &itinerary.legs {
                if let (Some(id), Some(chateau)) = (leg.start_stop_id(), leg.start_stop_chateau()) {
                    stop_ids.push((chateau.clone(), id.clone()));
                }
                if let (Some(id), Some(chateau)) = (leg.end_stop_id(), leg.end_stop_chateau()) {
                    stop_ids.push((chateau.clone(), id.clone()));
                }
                if let (Some(id), Some(chateau)) = (leg.route_id(), leg.chateau()) {
                    route_ids.push((chateau.clone(), id.clone()));
                } else {
                    println!(
                        "Leg missing route info: Mode={:?}, RouteID={:?}, Chateau={:?}",
                        leg.mode(),
                        leg.route_id(),
                        leg.chateau()
                    );
                }
            }
        }
        println!("Hydrating routes {:?}", route_ids);

        // Hydrate
        if let Ok(stop_map) = self.hydrator.hydrate_stops(stop_ids).await {
            for itinerary in &mut result.itineraries {
                for leg in &mut itinerary.legs {
                    if let (Some(id), Some(chateau)) =
                        (leg.start_stop_id(), leg.start_stop_chateau())
                    {
                        if let Some(name) = stop_map.get(&(chateau.clone(), id.clone())) {
                            leg.set_start_stop_name(Some(name.clone()));
                        }
                    }
                    if let (Some(id), Some(chateau)) = (leg.end_stop_id(), leg.end_stop_chateau()) {
                        if let Some(name) = stop_map.get(&(chateau.clone(), id.clone())) {
                            leg.set_end_stop_name(Some(name.clone()));
                        }
                    }
                }
            }
        }

        if let Ok(route_map) = self.hydrator.hydrate_routes(route_ids).await {
            for itinerary in &mut result.itineraries {
                for leg in &mut itinerary.legs {
                    if let (Some(id), Some(chateau)) = (leg.route_id(), leg.chateau()) {
                        if let Some(name) = route_map.get(&(chateau.clone(), id.clone())) {
                            leg.set_route_name(Some(name.clone()));
                        }
                    }
                }
            }
        }

        println!(
            "EdelweissService returning {} itineraries",
            result.itineraries.len()
        );
        result
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    let graph_dir = std::env::var("GRAPH_DIR").unwrap_or_else(|_| "graphs".to_string());

    println!("Initializing Edelweiss Routing Engine...");
    let mut graph_manager = GraphManager::new();
    // Don't fail if directory doesn't exist, just warn (for testing)
    if let Err(e) = graph_manager.load_from_directory(&graph_dir) {
        println!("Warning: Failed to load graphs: {}", e);
    }
    let graph_arc = Arc::new(graph_manager);

    println!("Connecting to database...");
    let pool = make_async_pool().await.map_err(|e| anyhow::anyhow!(e))?;
    let hydrator_arc = Arc::new(Hydrator::new(pool));

    let server_addr = "0.0.0.0:9090";
    let listener = TcpListener::bind(server_addr).await?;
    println!("Listening on {}", server_addr);

    loop {
        let (stream, _) = listener.accept().await?;
        let server = EdelweissServer {
            graph: graph_arc.clone(),
            hydrator: hydrator_arc.clone(),
        };
        tokio::spawn(async move {
            let transport = tarpc::serde_transport::Transport::from((
                stream,
                tarpc::tokio_serde::formats::Json::default(),
            ));
            let channel = server::BaseChannel::with_defaults(transport);
            channel
                .execute(server.serve())
                .for_each(|response| async move {
                    tokio::spawn(response);
                })
                .await;
        });
    }
}
