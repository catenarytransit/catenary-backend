## Architecture

Catenary Backend is a distributed system comprised of microservices operating in Kubernetes. The system is designed for fault tolerance, high-avaliability, and native execution speed in x86-64 using the Rust systems programming language.

- **Maple**: GTFS Downloader and ingestion engine
- **Prairie**: Routing Preprocessor and execution engine (Research and design in progress)
- **Kactus**: Distributed system to query for GTFS-rt and other realtime data
- **Aspen**: Processing of realtime data and dynamic insertion into other engines
- **Spruce**: API server for frontend to perform queries

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
We've switched to sqlx for our queries. For development, you'll need to know these few commands.

0. `cargo install sqlx-cli`
Install SQLx onto your system. You'll only need to do this once / when there are sqlx updates.

1. `cargo sqlx database drop`
This drops your old development database so you can create a new one.
If this doesn't work, you can run `DROP DATABASE catenary WITH (FORCE)` in the psql shell.

2. `cargo sqlx database create`
This creates a new sqlx database

3. `cargo sqlx migrate run`
This initialises the base tables and functions required to ingest our dataset.

4. `cargo sqlx prepare`
This will compile your sql code into .sqlx representation into the folder `.sqlx`, so that future code compilation no longer requires sqlx. The `.sqlx` folder is written into the Git history to assist other contributors without access to a working database. If a merge conflict occurs, the folder should be deleted and regenerated before reuploading.

### Common Database debugging

Is Postgis not installing? This page may be helpful: https://trac.osgeo.org/postgis/wiki/UsersWikiPostGIS3UbuntuPGSQLApt