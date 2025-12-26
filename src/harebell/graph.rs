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
}

#[derive(Debug, Clone)]
pub struct LineOnEdge {
    pub line_id: String,
    pub color: String,
    pub chateau_id: String,
    pub route_id: String,
    pub weight: u32,
}

pub struct RenderGraph {
    pub nodes: HashMap<i64, Node>,
    pub restrictions: HashMap<(usize, usize), Vec<(String, String)>>,
    pub edges: Vec<Edge>,
    pub tree: RTree<GeomWithData<rstar::primitives::Rectangle<[f64; 2]>, usize>>,
    pub node_tree: RTree<GeomWithData<[f64; 2], i64>>,
    pub collapsed_lines: HashMap<String, Vec<LineOnEdge>>,
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
        }
    }
}
