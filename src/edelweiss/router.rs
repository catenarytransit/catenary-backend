use catenary::routing_common::api::{Place, RoutingRequest, RoutingResult};
use catenary::routing_common::graph_loader::GraphManager;
use catenary::routing_common::osm_router::OsmRouter;
use std::sync::Arc;

pub struct Router<'a> {
    graph: &'a GraphManager,
}

impl<'a> Router<'a> {
    pub fn new(graph: &'a GraphManager) -> Self {
        Self { graph }
    }

    pub fn route(&self, req: &RoutingRequest) -> RoutingResult {
        // 1. Resolve Region
        let (start_lat, start_lon) = match &req.origin {
            Place::Coordinate { lat, lon } => (*lat, *lon),
            _ => {
                println!("Non-coordinate origins not supported via this interface yet.");
                return RoutingResult {
                    itineraries: vec![],
                };
            }
        };

        let (end_lat, end_lon) = match &req.destination {
            Place::Coordinate { lat, lon } => (*lat, *lon),
            _ => {
                println!("Non-coordinate origins not supported via this interface yet.");
                return RoutingResult {
                    itineraries: vec![],
                };
            }
        };

        // Determine if they fall in the same region.
        let sn_reg = self.graph.get_region_for_point(start_lat, start_lon);
        let en_reg = self.graph.get_region_for_point(end_lat, end_lon);

        if let (Some(sn), Some(en)) = (sn_reg, en_reg) {
            if sn == en {
                // Intra-region routing
                if let Ok(loaded_graph) = self.graph.get_or_load_region(&sn) {
                    let osm_router = OsmRouter::new(loaded_graph);
                    match osm_router.route(req) {
                        Ok(res) => return res,
                        Err(e) => {
                            println!("OsmRouter error: {}", e);
                            return RoutingResult {
                                itineraries: vec![],
                            };
                        }
                    }
                } else {
                    println!("Failed to load graph for region: {}", sn);
                }
            } else {
                println!(
                    "Inter-region OSM routing is not currently supported ({} -> {})",
                    sn, en
                );
            }
        } else {
            println!("Origin or Destination falls outside of any loaded region bounding box.");
        }

        RoutingResult {
            itineraries: vec![],
        }
    }
}
