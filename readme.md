## Architecture

Catenary Backend is a distributed system comprised of microservices operating in Kubernetes. The system is designed for fault tolerance, high-avaliability, and native execution speed in x86-64 using the Rust systems programming language.

- **Maple**: GTFS Downloader and ingestion engine into postgres
- **Edelweiss**: Routing execution engine (In progress, see also https://github.com/catenarytransit/routing)
- **Avens**: Osm Preprocessor and graph generator for routing
- **Alpenrose**: Distributed system to ingest GTFS-rt and other realtime data (Rose des Alpes), successor to Kactus.
- **Aspen**: Processing of realtime data and dynamic insertion into other engines
   Submodule Pando is used for distribution management
- **
- **Foxglove**: Map tile geometry server, will serve line ordering optimised graph maps (LOOM) in the future. [not started yet]
- **Spruce**: Websocket server for frontend to stream data to and from backend, including realtime locations, stop times (not started yet)
- **Birch**: HTTP API server

The kubernetes configuration is generated using Helm templates. See Helm's documentation for further information on that.

The code is heavily commented, go to each folder in src for more information.

### Submodules maintained 
- **Dmfr dataset reader**: reads data from transitland-atlas into raw structs https://docs.rs/dmfr-dataset-reader/latest/dmfr_dataset_reader/
- **[Château](https://github.com/catenarytransit/chateau)**: Associates feeds with operators and vise versa using depth first search in knowledge graph
- **[Amtrak GTFS rt](https://github.com/catenarytransit/amtrak-gtfs-rt)**: Conversion of proprietary realtime data from amtrak's website into gtfs-rt.
- **Chicago GTFS Rt**: conversion of proprietary Chicago CTA data to GTFS realtime
- **Rtc Québec GTFS RT**: conversion of proprietary app RTC Nomade to GTFS realtime
- **[Via Rail GTFS rt](https://github.com/catenarytransit/via-rail-gtfsrt)**: Conversion of Via Rail tracking to GTFS Realtime.

## Install Dependencies

```bash
sudo apt install -y postgresql-common
sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh
sudo apt install libprotoc-dev protobuf-compiler build-essential gcc pkg-config libssl-dev postgresql postgresql-17 postgresql-17-postgis postgresql-contrib unzip wget cmake openssl libpq-dev
```

### Enable PostGis

```sql
CREATE EXTENSION postgis;
```

## For Contributors

Good commit messages are required to contribute to this project.

### Installation of Postgres

See https://www.postgresql.org/download

PostGIS is also required like 
```bash
sudo apt install postgresql-16-postgis-3
```

See https://trac.osgeo.org/postgis/wiki/UsersWikiPostGIS3UbuntuPGSQLApt for more instructions

### SQL notes
We've switched to diesel for our queries. Read the diesel documentation to learn how to use it.
https://diesel.rs/guides/getting-started.html

Lib PQ is also required to install the diesel cli. Only postgres is required.
Example
```bash
sudo apt-get install libpq-dev
cargo install diesel_cli --no-default-features --features postgres
```

### Common Database debugging

Is Postgis not installing? This page may be helpful: https://trac.osgeo.org/postgis/wiki/UsersWikiPostGIS3UbuntuPGSQLApt

### Updating transitland submodules

```bash
git submodule update --rebase --remote
```

### Style Guide

Code should be formatted with `cargo fmt` and be well documented.
The following `cargo clippy` rules are enforced.

```rs
#![deny(
    clippy::mutable_key_type,
    clippy::map_entry,
    clippy::boxed_local,
    clippy::let_unit_value,
    clippy::redundant_allocation,
    clippy::bool_comparison,
    clippy::bind_instead_of_map,
    clippy::vec_box,
    clippy::while_let_loop,
    clippy::useless_asref,
    clippy::repeat_once,
    clippy::deref_addrof,
    clippy::suspicious_map,
    clippy::arc_with_non_send_sync,
    clippy::single_char_pattern,
    clippy::for_kv_map,
    clippy::let_unit_value,
    clippy::let_and_return,
    clippy::iter_nth,
    clippy::iter_cloned_collect,
    clippy::bytes_nth,
    clippy::deprecated_clippy_cfg_attr,
    clippy::match_result_ok,
    clippy::cmp_owned,
    clippy::cmp_null,
    clippy::op_ref,
    clippy::useless_vec,
    clippy::module_inception
)]
```

### Future potential submodule names

- **Truffle** reachability analysis
- **Cypress**
- **Chinese plum** (needs to pick better syonym for it)