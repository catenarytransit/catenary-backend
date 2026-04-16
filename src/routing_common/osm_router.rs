use crate::routing_common::api::{
    Itinerary, Leg, OsmLeg, RoutingRequest, RoutingResult, TravelMode,
};
use crate::routing_common::dijkstra::{self, RoutePath};
use crate::routing_common::graph_loader::LoadedGraph;
use crate::routing_common::profiles::{FootProfile, SearchProfile};
use crate::routing_common::types::CostT;
use std::sync::Arc;

pub struct OsmRouter {
    graph: Arc<LoadedGraph>,
}

impl OsmRouter {
    pub fn new(graph: Arc<LoadedGraph>) -> Self {
        Self { graph }
    }

    pub fn route(&self, req: &RoutingRequest) -> anyhow::Result<RoutingResult> {
        let (start_lat, start_lon) = match &req.origin {
            crate::routing_common::api::Place::Coordinate { lat, lon } => (*lat, *lon),
            _ => anyhow::bail!(
                "Non-coordinate origins not supported for intra-region OSM routing fallback yet."
            ),
        };

        let (end_lat, end_lon) = match &req.destination {
            crate::routing_common::api::Place::Coordinate { lat, lon } => (*lat, *lon),
            _ => anyhow::bail!(
                "Non-coordinate destinations not supported for intra-region OSM routing fallback yet."
            ),
        };

        let start_candidates =
            self.graph
                .lookup
                .match_location(&self.graph.routing, start_lat, start_lon, 5000.0);
        let start_node = start_candidates
            .first()
            .and_then(|c| c.left.as_ref().map(|n| n.node))
            .ok_or_else(|| anyhow::anyhow!("Could not snap origin to routing graph"))?;

        let end_candidates =
            self.graph
                .lookup
                .match_location(&self.graph.routing, end_lat, end_lon, 5000.0);
        let end_node = end_candidates
            .first()
            .and_then(|c| c.left.as_ref().map(|n| n.node))
            .ok_or_else(|| anyhow::anyhow!("Could not snap destination to routing graph"))?;

        let max_cost = req.max_travel_time.unwrap_or(36000) as CostT;

        let path_opt = match req.mode {
            TravelMode::Bike => dijkstra::route::<FootProfile>(
                // WARNING: Falling back to FootProfile for Bike
                &Default::default(),
                &self.graph.routing,
                start_node,
                end_node,
                max_cost,
            ),
            TravelMode::Walk => dijkstra::route::<FootProfile>(
                &Default::default(),
                &self.graph.routing,
                start_node,
                end_node,
                max_cost,
            ),
            TravelMode::Transit => anyhow::bail!("Transit routing temporarily disabled"),
        };

        let path = path_opt.ok_or_else(|| anyhow::anyhow!("No route found"))?;

        let geometry: Vec<(f64, f64)> = path
            .nodes
            .iter()
            .map(|&idx| {
                let pt = self.graph.routing.get_node_pos(idx);
                (pt.lat(), pt.lng())
            })
            .collect();

        // Compute start/end times
        let duration_seconds = path.total_cost as u64;
        let (start_time, end_time) = if req.is_departure_time {
            (req.time, req.time + duration_seconds)
        } else {
            (req.time.saturating_sub(duration_seconds), req.time)
        };

        let leg = Leg::Osm(OsmLeg {
            start_time,
            end_time,
            mode: req.mode.clone(),
            start_stop_id: None,
            end_stop_id: None,
            start_stop_chateau: None,
            end_stop_chateau: None,
            start_stop_name: Some("Origin".to_string()),
            end_stop_name: Some("Destination".to_string()),
            duration_seconds,
            geometry,
        });

        let itinerary = Itinerary {
            start_time,
            end_time,
            duration_seconds,
            transfers: 0,
            reliability_score: 1.0,
            legs: vec![leg],
        };

        Ok(RoutingResult {
            itineraries: vec![itinerary],
        })
    }
}
