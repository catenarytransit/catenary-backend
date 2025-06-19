## Architecture

Catenary Backend is a distributed system comprised of microservices operating in Kubernetes. The system is designed for fault tolerance, high-avaliability, and native execution speed in x86-64 using the Rust systems programming language.

- **Maple**: GTFS Downloader and ingestion engine
- **Prairie**: Routing Preprocessor and execution engine (In progress, see https://github.com/catenarytransit/catenary-routing-engine)
- **Alpenrose**: Distributed system to ingest GTFS-rt and other realtime data (Rose des Alpes), successor to Kactus.
- **Aspen**: Processing of realtime data and dynamic insertion into other engines
- **Edelweiss**: Map tile geometry server, will serve line ordering optimised graph maps (LOOM) in the future. [not started yet]
- **Spruce**: Websocket server for frontend to stream data to and from backend, including realtime locations, stop times (not started yet)
- **Birch**: HTTP API server

The kubernetes configuration is generated using Helm templates. See Helm's documentation for further information on that.

The code is heavily commented, go to each folder in src for more information.

### Submodules maintained 
- **Dmfr dataset reader**: reads data from transitland-atlas into raw structs https://docs.rs/dmfr-dataset-reader/latest/dmfr_dataset_reader/
- **[Château](https://github.com/catenarytransit/chateau)**: Associates feeds with operators and vise versa using depth first search in knowledge graph
- **[Amtrak GTFS rt](https://github.com/catenarytransit/amtrak-gtfs-rt)**: Conversion of proprietary realtime data from amtrak's website into gtfs-rt.
- **Zotgtfs**: conversion of Transloc data and hand typed schedules from Anteater Express to GTFS schedule and realtime.
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

It is expressly forbidden to contribute to Catenary any content that has been created with the assistance of Natural Language Processing artificial intelligence tools. This motion can be revisited, should a case been made over such a tool that does not pose copyright, ethical and quality concerns.

Rationale:

1.     Copyright concerns. At this point, the regulations concerning copyright of generated contents are still emerging worldwide. Using such material could pose a danger of copyright violations, but it could also weaken Gentoo claims to copyright and void the guarantees given by copyleft licensing.
2. Quality concerns. Popular LLMs are really great at generating plausibly looking, but meaningless content. They are capable of providing good assistance if you are careful enough, but we can't really rely on that. At this point, they pose both the risk of lowering the quality of Gentoo projects, and of requiring an unfair human effort from developers and users to review contributions and detect the mistakes resulting from the use of AI.
3. Ethical concerns. The business side of AI boom is creating serious ethical concerns. Among them:
        - Commercial AI projects are frequently indulging in blatant copyright violations to train their models.
        - Their operations are causing concerns about the huge use of energy and water.
        - The advertising and use of AI models has caused a significant harm to employees and reduction of service quality.
        - LLMs have been empowering all kinds of spam and scam efforts.

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
