Algorithm Authors: Wen, Chelsea <chelsea@catenarymaps.org>; Chin, Kyler <kyler@catenarymaps.org>

# Algorithm Overview

The Catenary routing algorithm is based on Transfer Patterns[[1]](#cite_1) and Scalable Transfer Patterns[[2]](#cite_2) with support for multimodal travel, additional data compression methods, and GTFS Realtime integration.

- OSM ("road network") and transit ("transit network") data are stored as separate graphs, but linked in memory and in practice
- Multimodality is supported through free-pathing through OSM, which will support pedestrian, cyclist, and motorist legs (anything based on the road network as opposed to transit network)

## 1. The Street Layer (OSM)
Handles the "First Leg" (Origin $\to$ Station) and "Last Leg" (Station $\to$ Destination).

- **Structure**: Standard road graph (Nodes = Intersections, Edges = Roads/Paths).
- **Algorithm**: Bidirectional Dijkstra.
- **Update Cycle**: Slow (Monthly/Quarterly). Streets rarely change.
- **Assumptions**:
      - Travellers will be able to detect and avoid temporary obstacles such as sidewalk closures.
      - If pedestrian/cyclist - friendly infrastructure does not exist, they will be pathed on the road, with penalty. USE ONE'S OWN DISCRETION. DO NOT FOLLOW MAP DIRECTIONS BLINDLY. SAFETY IS PRIORITY. 

## 2. The Transit Layer (GTFS + Transfer Patterns)
Handles complex "Station-to-Station" routing using Scalable Transfer Patterns.

- **Structure**: Compressed Timetable Chunks (Nodes = Stops, Edges = Trips).
- **Algorithm**: Transfer Pattern DAG Search.
- **Update Cycle**: Fast (Daily to Hourly). Schedules change constantly; valid dates expire.

**Note**: Far transfers (up to a few km in suburban areas or enclaves) are allowed to enable cyclists to save time. This can be calculated by identifying sparse areas with poor frequency. 
We plan on making these metrics (travel speed, transfer distance, etc.) customizable to the user to match current road conditions, mood, state of health, etc. 

---

# Data Generation & Ingestion Pipeline

## Chunk-Based Generation
To avoid memory exhaustion (e.g., processing `planet.pbf`), generation is performed chunk-by-chunk rather than via monolithic processing.

### OSM Data
- **Source**: The entire planet file is not processed at once. This would be impractical and also incredibly diffifcult to parse through and query.
- **Strategy**: **Slippy Map Tile Grid (Quadtree)**
    - **Concept**: The world is divided into tiles using the standard Web Mercator (XYZ) system.
    - **Variable Density**:
        - **Dense Areas (e.g., NYC, London)**: High zoom levels (e.g., Z14) are used to keep file sizes manageable.
        - **Sparse Areas (e.g., Rural Midwest)**: Lower zoom levels (e.g., Z10 or Z8) are used to cover large areas without creating excessive empty files.
    - **Implementation**:
        - A **Quadtree** or **Tile Index** maps Lat/Lon coordinates to specific `chunk_x_y_z.pbf` files.
        - This significantly reduces the file count compared to a fixed-grid approach while maintaining performance in dense zones.
    - **Boundary Handling**:
        - **Flags**: Edges crossing tile boundaries are NOT cut. Taking inspiration from the Arc-Flags[[3]](#cite_3) methodology, these "crossings" are marked with a special `EDGE_FLAG_BORDER` flag, such that when the routing algorithm encounters an edge with this flag, it knows to fetch the next chunk to continue traversal.
        - **Avoid Overhang**: We do *not* use a bounding box with overhang, as this creates duplicate data and complicates graph connectivity.

### Public Transport Graph
- **Strategy**: **Merge-Based Clustering**.
- **Scope**: Graphs are generated per region/chateau to bound memory usage.
- **Clustering**:
    - Stops are partitioned into **Clusters** to minimize the number of cut edges (trips crossing boundaries) while maintaining a balanced cluster size (e.g., 500-1000 stops).
    - **Algorithm**: Greedy Merge-Based Clustering.
        1. Initialize each stop as a singleton cluster.
        2. Calculate edge weights between clusters (number of trips connecting them).
        3. Iteratively merge the pair of clusters with the highest connectivity, provided the merged size does not exceed the limit.
        4. Result: Convex-like, highly connected clusters.
    - **Outcome**: This process naturally identifies **Border Nodes** (stops with edges crossing cluster boundaries), which become part of the Global Routing graph.

## Storage & Trigger Mechanism
- **Concept**: Data is organized by **Chateau** (list of agencies sharing a GTFS feed).
- **Persistence**: Raw data is stored in **Postgres** (refer to `models.rs`).
- **Pipeline**:
    1. **Ingest**: Load GTFS/Realtime data into Postgres (maple submodule).
    2. **Trigger**: Completion of ingest triggers a generation job.
    3. **Generation**: Data is selected from Postgres to generate routing structures.
    4. **Alignment**: Data structures are aligned for Protobuf/Postgres, specifically handling **trip diffs** to ensure consistency.

---

# Data Structures

## Compressed Timetable
A variant of Trip-Based Routing data structures, organized by **Trip Patterns**.

**Trip Pattern**: A collection of trips that visit the exact same sequence of stops.
- `route_id`: Pointer to static route info.
- `stop_indices`: Sequence of stops in this pattern.
- `trips`: List of trips sorted by time.

**Compressed Trip**:
- `start_time`: Absolute epoch or midnight-offset integer.
- `delta_pointer`: Pointer to the specific timing array (shared `time_deltas`).
- `service_mask`: Bitmask for active days.

---

# Routing Hierarchy

We follow the hierarchy approach given by Bast et al. in Transfer Patterns[[1]](#cite_1) and Scalable Transfer Patterns[[2]](#cite_2): 

1. **Local Route (Intra-Cluster)**:
    - Computed using **Trip-Based Routing (TB)** on the local cluster graph.
    - Used when Source and Destination are in the same cluster.
2. **Border Route (Inter-Cluster / Inter-Chateau)**:
    - **Inter-Chateau Transfers**: Pre-computed transfers between overlapping or nearby Chateaus (e.g., LA Metro $\leftrightarrow$ Metrolink $\leftrightarrow$ Amtrak).
    - **Algorithm**: Spatial join on stops from different Chateaus. If distance < threshold (e.g., 500m), create a "Transfer Edge".
    - **Border Nodes**: Stops that connect different clusters (partitions). These are the "gates" between partitions.
    - **Hubs**: Major transfer points (high centrality) used for global routing optimization.
3. **Global Route (Long-Distance / Continent Level)**:
    - **Scope**: Computed per **Continent** (e.g., North America, Japan, Australia) to manage scale.
    - **Structure**: A DAG connecting **Global Nodes** (Hubs + Border Nodes) across the continent.
    - **Pre-computation**:
        - **Hub Selection (Centrality)**:
            - Hubs are identified by running random shortest path queries (Time-Dependent Centrality).
            - **Self-Transfer Penalty**: We explicitly penalise "self-transfers" (transfers between the same route/pattern) to prevent stops on loop routes from being falsely identified as major hubs. This ensures selected hubs are true interchange points between different services.
        - **Border Nodes**: Derived from the clustering process (stops with cross-partition links).
        - **Global Graph**: Constructed from the union of Hubs and Border Nodes.
        - **Pattern Generation**: Profile Search (CSA/Raptor) is run between all Global Nodes to find optimal long-distance transfer sequences.

---

# Serialization & Loading

- Each chunk is serialized into a separate file (e.g., `chunk_12.pbf` using Protocol Buffers or FlatBuffers).
- Boundary definitions (links to other chunks) must be stored.
- **Memory Mapping (mmap)**: Used for efficient loading. For a query starting at Stop A (Chunk 1) and ending at Stop B (Chunk 5), only `chunk_1.pbf`, `chunk_5.pbf`, and `global_patterns.pbf` are loaded/mmapped.
- Intermediate chunks are not loaded; global patterns indicate specific transfers to check.

---

# Handling Trip Modifications

Strategy for integrating real-time changes (delays, cancellations, etc.):

- **Data Source**: Trip update data is fetched from the **Aspen microservice** RT database in the **Catenary** project.
- **Integration**:
    - The Aspen service acts as the source of truth for real-time status.
    - During routing or graph updates, the Aspen database is queried to retrieve active trip modifications.
    - These modifications are applied to the static schedule data to ensure routes reflect current conditions.

---

# Realtime Architecture

Since the static schedule is immutable within binary graph chunks, GTFS-Realtime updates are handled via an **Overlay Pattern**.

- **Base Layer (Read-Only)**: `trip_starts` and `deltas` from the graph chunk (derived from Postgres).
- **Overlay Layer (Read-Write, Volatile)**: A HashMap or Sparse Array in RAM.
    - **Key**: `trip_id` (or integer index).
    - **Value**: `delay_seconds`.

## Algorithm Logic

```rust
// Pseudocode
fn get_arrival(trip_index: usize, stop_sequence: usize) -> i64 {
    // 1. Get Static Time (Nanosecond lookup from mmapped graph)
    let static_start = graph.trip_starts[trip_index];
    let delta = graph.deltas[trip_index][stop_sequence];
    
    // 2. Get Realtime Delay (Fast hashmap lookup)
    let delay = realtime_map.get(&trip_index).unwrap_or(&0);
    
    // 3. Calculate
    static_start + delta + delay
}
```

## Multimodal Integration (OSM + GTFS)
OSM and GTFS graphs are kept loosely coupled within chunks rather than merged into a single monolithic graph.

- **Access Legs**: Walking paths from every OSM node to the nearest Transit Stops are pre-computed and stored as "access edges" with a time cost.
- **Transfers**: Within a chunk, walking times between nearby stops are pre-calculated (e.g., Stop A to Stop B is a 3-min walk) and stored as static edges in the chunk file.

## Calculating with GTFS Realtime (Delays & Cancellations)
Transfer Patterns are expensive to compute and cannot be re-run frequently. However, they are robust to delays; the sequence of transfers rarely changes due to minor delays, only the viability of the sequence.

### Query-Time Adjustment Strategy
1. **Ingest**: Maintain a lightweight, in-memory Delay Map (`Map<TripID, DelaySeconds>`), Set (`Set<CancelledTripIDs>`), and Set (`Set<(TripID, StopSequence)>` for cancelled stops), updated from the GTFS-RT feed.
2. **Query Execution**:
    - Retrieve the pre-computed Transfer Pattern DAG for the requested O/D pair.
    - Traverse the DAG to find the actual departure time.
    - **Check Cancellation**: 
        - Before evaluating Trip T, check if T is in `CancelledTripIDs`. If so, ignore it.
        - Check if the specific stop S on Trip T is in `CancelledStopIDs`. If so, treat as if the stop does not exist for this trip (cannot board or alight).
    - **Apply Delay**: When looking up the arrival time, read Base Schedule Time (from compressed array) + DelaySeconds (from map).
    - **Example**: Scheduled arrival 14:00 (base) + 300s (delta). Map indicates +120s delay. Effective arrival = 14:07.
3. **On-the-Fly Re-routing**: If a delay breaks a connection in the pre-computed pattern, the algorithm naturally "falls through" to the next available trip in the sequence. If all patterns fail, it falls back to a standard A* search on the local graph.

### Additional notes
"The graph partitioning problem asks for a balanced partition that minimizes the weighted sum of all cut edges." [[4]](#cite_4)

# Bibliography
<a name="cite_1"></a>
[1]: H. Bast et al., “Fast Routing in Very Large Public Transportation Networks Using Transfer Patterns,” ESA 2010, pp. 290–301, Sep. 2010, doi: https://doi.org/10.1007/978-3-642-15775-2_25.  
<a name="cite_2"></a>
[2]: H. Bast, M. Hertel, and S. Storandt, “Scalable Transfer Patterns,” 2016 Proceedings of the Eighteenth Workshop on Algorithm Engineering and Experiments (ALENEX), Dec. 2015, doi: https://doi.org/10.1137/1.9781611974317.2.  
<a name="cite_3"></a>
[3]: U. Lauther, “An Extremely Fast, Exact Algorithm for Finding Shortest Paths in Static Networks with Geographical Background,” Siemens AG, Corporate Technology Software & Engineering, vol. 6, pp. 219–230, May 2004.  
<a name="cite_4"></a>
[4]: E. Großmann, J. Sauer, C. Schulz, and P. Steil, “Arc-Flags Meet Trip-Based Public Transit Routing,” arXiv (Cornell University), Feb. 2023, doi: https://doi.org/10.48550/arxiv.2302.07168.  
