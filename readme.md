## Architecture

Catenary Backend is a distributed system comprised of microservices operating in Kubernetes. The system is designed for fault tolerance, high-avaliability, and native execution speed in x86-64 using the Rust systems programming language.

- **Maple**: GTFS Downloader and ingestion engine
- **Prairie**: Routing Preprocessor and execution engine (Research and design in progress)
- **Kactus**: Distributed system to query for GTFS-rt and other realtime data
- **Aspen**: Processing of realtime data and dynamic insertion into other engines
- **Spruce**: Websocket server for frontend to stream data to and from backend
- **Birch**: General API server

The kubernetes configuration is generated using Helm templates. See Helm's documentation for further information on that.

The code is heavily commented, go to each folder in src for more information.

## Install Dependencies

```bash
sudo apt install protobuf-compiler build-essential gcc pkg-config libssl-dev postgresql unzip wget
```

## Loading in Data
Loading in data into the Postgres database is a multistep process. Ensure your postgres database is working and your password is set correctly.

### Download the Transitland repo
Transitland acts as an initial source of knowledge for Catenary-Backend, and associates static feeds and realtime feeds together. We run a fork of transitland to add additional datasets that the upstream maintainers may not approve of.
Download and Update it via:
```bash
git submodule init && git submodule update
```

If you already have it, remember to git pull / merge changes
To do this, cd into the folder `transitland-atlas` and run `git pull`

### Install Systemd Service
```bash
sudo cp transitbackend.service /etc/systemd/system/transitbackend.service
sudo systemctl daemon-reload
sudo systemctl enable --now transitbackend.service
```

## For Contributors

For unix users, running `git config core.hooksPath .githooks` is recommended.
Good commit messages are required to contribute to this project.

No option exists for Windows users at the moment. Please try WSL Ubuntu for the moment. We're working on adding this.

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