use crate::routing_common::types::*;
use crate::routing_common::ways::*;
use rstar::{RTree, RTreeObject, AABB};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WayEnvelope {
    way: WayIdx,
    min_lat: f64,
    min_lng: f64,
    max_lat: f64,
    max_lng: f64,
}

impl RTreeObject for WayEnvelope {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_corners([self.min_lng, self.min_lat], [self.max_lng, self.max_lat])
    }
}

/// Candidate node on a matched way, with cost to reach it.
#[derive(Debug, Clone)]
pub struct NodeCandidate {
    pub node: NodeIdx,
    pub level: Level,
    pub dist_to_node: f64,
    pub cost: CostT,
}

/// A matched way near a query point.
#[derive(Debug, Clone)]
pub struct WayCandidate {
    pub dist_to_way: f64,
    pub way: WayIdx,
    pub left: Option<NodeCandidate>,
    pub right: Option<NodeCandidate>,
}

/// Spatial lookup index over way bounding boxes, mirroring OSR's lookup struct.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Lookup {
    rtree: RTree<WayEnvelope>,
}

impl Lookup {
    pub fn build(graph: &RoutingGraph) -> Self {
        let mut envelopes = Vec::with_capacity(graph.n_ways());
        for w in 0..graph.n_ways() {
            let nodes = &graph.way_nodes[w];
            if nodes.is_empty() {
                continue;
            }

            let mut min_lat = f64::MAX;
            let mut max_lat = f64::MIN;
            let mut min_lng = f64::MAX;
            let mut max_lng = f64::MIN;

            for node in nodes {
                let pos = graph.node_positions[node.idx()];
                let lat = pos.lat();
                let lng = pos.lng();
                min_lat = min_lat.min(lat);
                max_lat = max_lat.max(lat);
                min_lng = min_lng.min(lng);
                max_lng = max_lng.max(lng);
            }

            envelopes.push(WayEnvelope {
                way: WayIdx(w as u32),
                min_lat,
                min_lng,
                max_lat,
                max_lng,
            });
        }

        Self {
            rtree: RTree::bulk_load(envelopes),
        }
    }

    /// Find the nearest way candidates to a lat/lng query point.
    pub fn match_location(
        &self,
        graph: &RoutingGraph,
        lat: f64,
        lng: f64,
        max_distance_m: f64,
    ) -> Vec<WayCandidate> {
        let approx_deg = max_distance_m / 111_000.0;
        let search_box = AABB::from_corners(
            [lng - approx_deg, lat - approx_deg],
            [lng + approx_deg, lat + approx_deg],
        );

        let query_point = Point::from_latlng(lat, lng);
        let mut candidates = Vec::new();

        for envelope in self.rtree.locate_in_envelope(&search_box) {
            let way = envelope.way;
            let way_nodes = &graph.way_nodes[way.idx()];
            if way_nodes.is_empty() {
                continue;
            }

            let mut best_dist = f64::MAX;
            let mut best_seg = 0usize;

            // Find closest segment
            for seg in 0..way_nodes.len().saturating_sub(1) {
                let p1 = graph.node_positions[way_nodes[seg].idx()];
                let p2 = graph.node_positions[way_nodes[seg + 1].idx()];
                let d = point_to_segment_dist(&query_point, &p1, &p2);
                if d < best_dist {
                    best_dist = d;
                    best_seg = seg;
                }
            }

            if best_dist > max_distance_m {
                continue;
            }

            // Left candidate: node at best_seg
            let left_node = way_nodes[best_seg];
            let left_dist = query_point.haversine_distance(&graph.node_positions[left_node.idx()]);

            // Right candidate: node at best_seg + 1
            let right = if best_seg + 1 < way_nodes.len() {
                let right_node = way_nodes[best_seg + 1];
                let right_dist =
                    query_point.haversine_distance(&graph.node_positions[right_node.idx()]);
                Some(NodeCandidate {
                    node: right_node,
                    level: Level::NO_LEVEL,
                    dist_to_node: right_dist,
                    cost: 0,
                })
            } else {
                None
            };

            candidates.push(WayCandidate {
                dist_to_way: best_dist,
                way,
                left: Some(NodeCandidate {
                    node: left_node,
                    level: Level::NO_LEVEL,
                    dist_to_node: left_dist,
                    cost: 0,
                }),
                right,
            });
        }

        candidates.sort_by(|a, b| a.dist_to_way.partial_cmp(&b.dist_to_way).unwrap());
        candidates
    }
}

fn point_to_segment_dist(p: &Point, a: &Point, b: &Point) -> f64 {
    let pa_lat = p.lat() - a.lat();
    let pa_lng = p.lng() - a.lng();
    let ba_lat = b.lat() - a.lat();
    let ba_lng = b.lng() - a.lng();

    let dot = pa_lat * ba_lat + pa_lng * ba_lng;
    let len_sq = ba_lat * ba_lat + ba_lng * ba_lng;

    if len_sq < 1e-20 {
        return p.haversine_distance(a);
    }

    let t = (dot / len_sq).clamp(0.0, 1.0);
    let proj = Point::from_latlng(a.lat() + t * ba_lat, a.lng() + t * ba_lng);
    p.haversine_distance(&proj)
}
