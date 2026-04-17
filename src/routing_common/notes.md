Algorithm Authors: Wen, Chelsea <chelsea@catenarymaps.org>; Chin, Kyler <kyler@catenarymaps.org>

# Binaries

1. **Avens**: OSM graph preprocessor.
2. **Gentian**: Zero-copy timetable compiler.
3. **Edelweiss**: The high-performance routing execution engine.

---

# Architecture

## 1. The Street Layer (Avens)
Handles multimodal first-mile, last-mile, and transfer footpaths.

- **Process**: `Avens` takes raw OpenStreetMap PBF files and pre-compiles them into chunked regional routing graphs.
- **R-Tree Index**: Regional graphs are indexed using a global R-Tree. Instead of loading a massive `planet.pbf` into memory, the system dynamically loads only the specific regional bounds necessary to fulfill a query.
- **Output**: Serialized binary graphs optimized for memory-mapped read access.

## 2. Transit Ingestion (Maple & PostgreSQL)
- Instead of reading GTFS files directly in the router, the **Maple** microservice ingests raw GTFS feeds and normalizes them into a **PostgreSQL** database, which is our unified source of truth, making it possible to handle data cleaning, deduplication, and cross-agency stop snapping (e.g., merging duplicate station nodes) before any routing structures are built. It also allows us to handle several feed versions from a single agency at once.

## 3. Timetable Compilation (Gentian)
Handles the "compile" step of the routing graphs.

- **Process**: `Gentian` pulls the cleaned transit stops, routes, and trips from PostgreSQL. 
- **Footpath Generation**: To compute transfer edges (walking distances between nearby transit stops), Gentian utilizes an R-Tree to find nearby coordinates, dynamically loads the corresponding **Avens** graphs, and routes footpaths directly on the OSM street network.
- **Output**: The output is a `timetable.bin` file containing tightly packed, contiguous arrays (Arrays of Routes, Stop Times, Footpaths, etc.). This file is written directly in the layout expected by the routing engine, forming a "zero-copy" timetable.

## 4. Routing Execution (Edelweiss)
The frontend API and execution layer.

- **Process**: `Edelweiss` Upon startup, reads the memory map (`mmap()`) on `timetable.bin`.
- **Algorithm**: Runs the Round-Based Public Transit Routing (RAPTOR) algorithm on the zero-copy transit graph, ensuring high-speed journey reconstruction.
- **Multimodal Routing**: For coordinates outside of transit stops, Edelweiss invokes `Avens`' dynamic regional graphs to calculate the first/last leg.

---

# Key Differences from Motis / Nigiri

While the Edelweiss/Gentian pipeline is based on Motis (Nigiri) to achieve similar zero-copy performance and memory efficiency, we have some differences:

1. **Decoupled Preprocessing via PostgreSQL**:
   - **Motis**: Reads raw GTFS/NeTEx and OSM straight from disk to build routing topologies in memory.
   - **Catenary**: Ingests everything into a persistent relational database (PostgreSQL via Maple) first. Gentian compiles the timetable from this normalized Postgres layer, allowing for heavy data-cleaning schemas and SQL-driven preprocessing prior to routing.

2. **Chunked OSM Graphs via Avens**:
   - **Motis**: Typically ingests a single OSM file for walking/biking transfers and pre / post queries.
   - **Catenary**: Avens breaks the OSM world into compiled regional binaries and uses an R-Tree to dynamically page in region graphs. This bounds memory consumption dramatically and multimodal setups viable by only mapping the regions that a query physically traverses. This also allows faster updates as regions can be updated in parallel in theory, though this is not implemented yet.

---

### Why is Transfer Patterns not currently being used?

Transfer Patterns by Hannah Bast, PhD et al, consumes an extremely high amount of preprocessing time for little query improvement, something impossible for our project's limited computer resources. We also would struggle to insert delays and cancellations into the query timetable structure. We found that in our own testing (Chelsea Wen), we were unable to process even small areas such as Conneticut without an Out-Of-Memory bottleneck. We may explore processing Transfer Patterns using memory mapped graphs in the future.

# Bibliography
[1] Round-Based Public Transit Routing, Karlsruhe Institute of Technology, Delling, Daniel; Pajor Thomas, Werneck Renato. https://www.microsoft.com/en-us/research/wp-content/uploads/2012/01/raptor_alenex.pdf
[2] Motis Project https://github.com/motis-project Felix Gündling, innovation in in-memory data stores for graphs