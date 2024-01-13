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

Example endpoints

`http://localhost:5401/getroutesperagency?feed_id=f-9mu-orangecountytransportationauthority`

`http://localhost:5401/gettrip?feed_id=f-9mu-orangecountytransportationauthority&trip_id=10995882`

## For Contributors

For unix users, running `git config core.hooksPath .githooks` is required.
Pull requests will not be merged without this.

No option exists for Windows users at the moment. Please try WSL Ubuntu for the moment. We're working on adding this.

### SQL notes
We've switched to sqlx for our queries. For development, you'll need to know these few commands.

1. `cargo sqlx database drop`
This drops your old development database so you can create a new one.

2. `cargo sqlx database create`
This creates a new sqlx database

3. `cargo sqlx migrate run`
This initialises the base tables and functions required to ingest our dataset.

4. `cargo sqlx prepare --workspace`
This will compile your sql code into .sqlx representation into the folder `.sqlx`, so that future code compilation no longer requires sqlx. The `.sqlx` folder is written into the Git history to assist other contributors without access to a working database. If a merge conflict occurs, the folder should be deleted and regenerated before reuploading.