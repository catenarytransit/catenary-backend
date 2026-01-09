use rstar::{
    RTree,
    primitives::{GeomWithData, Line},
};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Node {
    pub id: i64,
    pub x: f64,
    pub y: f64,
    pub name: Option<String>,
    pub is_cluster: bool,
}

#[derive(Debug, Clone)]
pub struct Edge {
    pub id: i64,
    pub from: i64,
    pub to: i64,
    pub lines: Vec<LineOnEdge>,
    pub geometry: Vec<[f64; 2]>,
    /// Direction flag for line ordering (matches C++ lnEdgParts.front().dir)
    /// Used to determine if positions should be inverted when checking crossings
    pub dir: bool,
}

#[derive(Debug, Clone)]
pub struct LineOnEdge {
    pub line_id: String,
    pub color: String,
    pub chateau_id: String,
    pub route_id: String,
    pub group_id: Option<String>,
    pub weight: u32,
}

pub struct RenderGraph {
    pub nodes: HashMap<i64, Node>,
    pub restrictions: HashMap<(usize, usize), Vec<(String, String)>>,
    pub edges: Vec<Edge>,
    pub tree: RTree<GeomWithData<rstar::primitives::Rectangle<[f64; 2]>, usize>>,
    pub node_tree: RTree<GeomWithData<[f64; 2], i64>>,
    pub collapsed_lines: HashMap<String, Vec<LineOnEdge>>,
    // Adjacency list for curve generation: NodeID -> List of Edge Indices connected to it
    pub node_to_edges: HashMap<i64, Vec<usize>>,
    // Route groups: (chateau, route_group_id) -> Vec of (route_id, color)
    // Routes with same short_name or color are grouped together
    pub route_groups: HashMap<(String, String), Vec<(String, String)>>,
}

impl RenderGraph {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            restrictions: HashMap::new(),
            edges: Vec::new(),
            tree: RTree::new(),
            node_tree: RTree::new(),
            collapsed_lines: HashMap::new(),
            node_to_edges: HashMap::new(),
            route_groups: HashMap::new(),
        }
    }

    pub fn rebuild_indices(&mut self) {
        // Rebuild Edge Tree
        let mut edge_items = Vec::with_capacity(self.edges.len());
        for (i, edge) in self.edges.iter().enumerate() {
            if edge.geometry.is_empty() {
                continue;
            }
            // Calculate bbox
            let mut min_x = f64::INFINITY;
            let mut min_y = f64::INFINITY;
            let mut max_x = f64::NEG_INFINITY;
            let mut max_y = f64::NEG_INFINITY;

            for p in &edge.geometry {
                if p[0] < min_x {
                    min_x = p[0];
                }
                if p[0] > max_x {
                    max_x = p[0];
                }
                if p[1] < min_y {
                    min_y = p[1];
                }
                if p[1] > max_y {
                    max_y = p[1];
                }
            }

            edge_items.push(GeomWithData::new(
                rstar::primitives::Rectangle::from_corners([min_x, min_y], [max_x, max_y]),
                i,
            ));
        }
        self.tree = RTree::bulk_load(edge_items);

        // Rebuild Node Tree
        let mut node_items = Vec::with_capacity(self.nodes.len());
        for (&id, node) in &self.nodes {
            node_items.push(GeomWithData::new([node.x, node.y], id));
        }
        self.node_tree = RTree::bulk_load(node_items);

        // Rebuild Node Adjacency
        self.node_to_edges.clear();
        for (idx, edge) in self.edges.iter().enumerate() {
            self.node_to_edges.entry(edge.from).or_default().push(idx);
            self.node_to_edges.entry(edge.to).or_default().push(idx);
        }
    }
}
