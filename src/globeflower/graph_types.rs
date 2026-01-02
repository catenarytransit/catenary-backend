// ===========================================================================
// Rc-based Line Graph (matches C++ UndirGraph<LineNodePL, LineEdgePL>)
// ===========================================================================
use ahash::{AHashMap, AHashSet};
use anyhow::Result;
use catenary::postgres_tools::CatenaryPostgresPool;
use geo::prelude::*;
use geo::{Coord, LineString as GeoLineString, Point as GeoPoint};
use postgis_diesel::types::{LineString, Point};
use rayon::prelude::*;
use rstar::{AABB, RTree, RTreeObject};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::{Rc, Weak};
use std::sync::{Arc, Mutex};
use catenary::graph_formats::NodeId;



pub type NodeRef = Rc<RefCell<LineNode>>;
pub type EdgeRef = Rc<RefCell<LineEdge>>;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Copy)]
pub enum RouteDirection {
    Both,
    Forward,  // From -> To
    Backward, // To -> From
}

/// Line occurrence on an edge (matches C++ LineOcc)
/// direction: None = bidirectional, Some(node) = directed towards that node
#[derive(Clone, Debug)]
pub struct LineOcc {
    pub line_id: (String, String),
    pub route_type: i32,
    /// Direction the line travels. None = bidirectional.
    /// Some(weak_ref) = directed towards the node pointed to.
    /// Using Weak to avoid reference cycles.
    pub direction: Option<Weak<RefCell<LineNode>>>,
}

impl LineOcc {
    pub fn new(
        line_id: (String, String),
        route_type: i32,
        direction: Option<Weak<RefCell<LineNode>>>,
    ) -> Self {
        Self {
            line_id,
            route_type,
            direction,
        }
    }

    /// Create a bidirectional line occurrence
    pub fn new_bidirectional(line_id: (String, String), route_type: i32) -> Self {
        Self {
            line_id,
            route_type,
            direction: None,
        }
    }
}

#[derive(Debug)]
pub struct LineNode {
    pub id: usize,
    pub pos: [f64; 2],
    pub adj_list: Vec<EdgeRef>,
    // Connection exceptions: lines that cannot continue between certain edge pairs at this node
    // Key: line_id, Value: Map of edge_id -> Set of forbidden target edge_ids
    pub conn_exc: AHashMap<(String, String), AHashMap<usize, AHashSet<usize>>>,
    // Track how many points have been merged into this node for weighted centroid
    pub merge_count: usize,
    // CRITICAL: Preserve original NodeId through cluster processing
    // If Some, use this NodeId when converting back to GraphEdge (boundary nodes)
    // If None, generate a new NodeId from self.id (internal nodes)
    pub original_node_id: Option<NodeId>,
}

impl LineNode {
    pub fn new(id: usize, pos: [f64; 2]) -> Self {
        Self {
            id,
            pos,
            adj_list: Vec::new(),
            conn_exc: AHashMap::new(),
            merge_count: 1, // Start at 1 - the initial point
            original_node_id: None,
        }
    }

    /// Create a new LineNode with an original NodeId preserved from input
    pub fn new_with_original(id: usize, pos: [f64; 2], original_node_id: NodeId) -> Self {
        Self {
            id,
            pos,
            adj_list: Vec::new(),
            conn_exc: AHashMap::new(),
            merge_count: 1,
            original_node_id: Some(original_node_id),
        }
    }

    pub fn get_deg(&self) -> usize {
        self.adj_list.len()
    }

    /// Add a connection exception - line cannot continue from edge_a to edge_b at this node
    pub fn add_conn_exc(&mut self, line_id: &(String, String), edge_a_id: usize, edge_b_id: usize) {
        // Store in both directions for fast lookup (matches C++)
        self.conn_exc
            .entry(line_id.clone())
            .or_default()
            .entry(edge_a_id)
            .or_default()
            .insert(edge_b_id);
        self.conn_exc
            .entry(line_id.clone())
            .or_default()
            .entry(edge_b_id)
            .or_default()
            .insert(edge_a_id);
    }

    /// Check if connection occurs (returns true unless exception exists)
    /// Matches C++ LineNodePL::connOccurs
    pub fn conn_occurs_check(
        &self,
        line_id: &(String, String),
        edge_a_id: usize,
        edge_b_id: usize,
    ) -> bool {
        if let Some(exc_map) = self.conn_exc.get(line_id) {
            if let Some(forbidden) = exc_map.get(&edge_a_id) {
                return !forbidden.contains(&edge_b_id);
            }
        }
        true // No exception found - connection is allowed
    }
}

#[derive(Debug)]
pub struct LineEdge {
    pub id: usize,
    pub from: NodeRef,
    pub to: NodeRef,
    pub routes: Vec<LineOcc>,
    pub geometry: Vec<Coord>,
}

impl LineEdge {
    pub fn get_other_nd(&self, n: &NodeRef) -> NodeRef {
        if Rc::ptr_eq(&self.from, n) {
            Rc::clone(&self.to)
        } else {
            Rc::clone(&self.from)
        }
    }
}

pub struct LineGraph {
    pub nodes: Vec<NodeRef>,
    pub next_node_id: usize,
    pub next_edge_id: usize,
}

impl LineGraph {
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            next_node_id: 0,
            next_edge_id: 0,
        }
    }

    /// Create a new LineGraph with a specified starting node ID.
    /// CRITICAL: Used to ensure node IDs are globally unique across partitions.
    pub fn new_with_start_id(start_node_id: usize) -> Self {
        Self {
            nodes: Vec::new(),
            next_node_id: start_node_id,
            next_edge_id: 0,
        }
    }

    pub fn add_nd(&mut self, pos: [f64; 2]) -> NodeRef {
        let id = self.next_node_id;
        self.next_node_id += 1;
        let node = Rc::new(RefCell::new(LineNode::new(id, pos)));
        self.nodes.push(Rc::clone(&node));
        node
    }

    /// Add a node with an original NodeId preserved from input edges.
    /// Use this for boundary nodes that need to maintain connectivity across clusters.
    pub fn add_nd_with_original(&mut self, pos: [f64; 2], original_node_id: NodeId) -> NodeRef {
        let id = self.next_node_id;
        self.next_node_id += 1;
        let node = Rc::new(RefCell::new(LineNode::new_with_original(
            id,
            pos,
            original_node_id,
        )));
        self.nodes.push(Rc::clone(&node));
        node
    }

    /// Get edge between two nodes, if it exists
    pub fn get_edg(&self, from: &NodeRef, to: &NodeRef) -> Option<EdgeRef> {
        let from_borrow = from.borrow();
        for edge in &from_borrow.adj_list {
            let edge_borrow = edge.borrow();
            if Rc::ptr_eq(&edge_borrow.to, to) || Rc::ptr_eq(&edge_borrow.from, to) {
                return Some(Rc::clone(edge));
            }
        }
        None
    }

    /// Add edge between two nodes
    pub fn add_edg(&mut self, from: &NodeRef, to: &NodeRef) -> EdgeRef {
        let edge_id = self.next_edge_id;
        self.next_edge_id += 1;
        let edge = Rc::new(RefCell::new(LineEdge {
            id: edge_id,
            from: Rc::clone(from),
            to: Rc::clone(to),
            routes: Vec::new(),
            geometry: vec![
                Coord {
                    x: from.borrow().pos[0],
                    y: from.borrow().pos[1],
                },
                Coord {
                    x: to.borrow().pos[0],
                    y: to.borrow().pos[1],
                },
            ],
        }));
        from.borrow_mut().adj_list.push(Rc::clone(&edge));
        to.borrow_mut().adj_list.push(Rc::clone(&edge));
        edge
    }

    pub fn get_nds(&self) -> &[NodeRef] {
        &self.nodes
    }

    pub fn num_edges(&self) -> usize {
        let mut count = 0;
        for node in &self.nodes {
            let node_borrow = node.borrow();
            for edge in &node_borrow.adj_list {
                // Count each edge once (when from == node)
                if Rc::ptr_eq(&edge.borrow().from, node) {
                    count += 1;
                }
            }
        }
        count
    }
    pub fn clear(&mut self) {
        // Break cycles manually
        for node in &self.nodes {
            node.borrow_mut().adj_list.clear();
        }
        self.nodes.clear();
    }
}