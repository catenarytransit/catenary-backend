use catenary::routing_common::api;
use catenary::routing_common::api::EdelweissService;
use catenary::routing_common::graph_loader::GraphManager;

//  Gingko is based on T-REX[1], an algorithmic approach to trip planning with public transit which uses the basis of TB (Trip-Based Public
//      Transit Routing) to answer queries for the optimal arrival time, by finding the transfers between trips for a set of journey (paths
//      from source to destination) while using a multi-level overlay focused on efficient partitioning and memory use.
//  
//  T-REX was originally written in C++[2].
//  
//  Testbench: Callichimaera, https://github.com/chw-03/callichimaera

// [1] J. Sauer, P. Steil, and S. Witt, “T-REX: Fast and Dynamic Journey Planning for Continental-Scale Public 
//     Transit Networks,” Apr. 24, 2026. Accessed: Oct. 11, 2026. [Online]. Available: https://arxiv.org/abs/2605.18778v1
// [2] https://github.com/PatrickSteil/TREX
